package modbus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

const (
	maxTCPFrameLength int = 1040 - mbapHeaderLength
	maxUDPFrameLength int = 64 - mbapHeaderLength
	mbapHeaderLength  int = 7
)

type TCPTransport struct {
	logger         *logger
	socket         net.Conn
	timeout        time.Duration
	maxFrameLength int
	lastTxnId      uint16
}

// Returns a new TCP transport.
func NewTCPTransport(socket net.Conn, timeout time.Duration, customLogger *log.Logger) (tt *TCPTransport) {
	tt = &TCPTransport{
		socket:  socket,
		timeout: timeout,
		logger:  newLogger(fmt.Sprintf("tcp-transport(%s)", socket.RemoteAddr()), customLogger),
	}
	switch socket.(type) {
	case *udpSockWrapper:
		tt.maxFrameLength = maxUDPFrameLength
	default:
		tt.maxFrameLength = maxTCPFrameLength
	}

	return
}

// Closes the underlying tcp socket.
func (tt *TCPTransport) Close() (err error) {
	err = tt.socket.Close()

	return
}

// Runs a request across the socket and returns a response.
func (tt *TCPTransport) ExecuteRequest(req *PDU) (res *PDU, err error) {
	// set an i/o deadline on the socket (read and write)
	err = tt.socket.SetDeadline(time.Now().Add(tt.timeout))
	if err != nil {
		return
	}

	// increase the transaction ID counter
	tt.lastTxnId++

	_, err = tt.socket.Write(tt.assembleMBAPFrame(tt.lastTxnId, req))
	if err != nil {
		return
	}

	res, err = tt.ReadResponse()

	return
}

// Reads a request from the socket.
func (tt *TCPTransport) ReadRequest() (req *PDU, err error) {
	var txnId uint16

	// set an i/o deadline on the socket (read and write)
	err = tt.socket.SetDeadline(time.Now().Add(tt.timeout))
	if err != nil {
		return
	}

	req, txnId, err = tt.readMBAPFrame(0)
	if err != nil {
		return
	}

	// store the incoming transaction id
	tt.lastTxnId = txnId

	return
}

// Writes a response to the socket.
func (tt *TCPTransport) WriteResponse(res *PDU) (err error) {
	_, err = tt.socket.Write(tt.assembleMBAPFrame(tt.lastTxnId, res))
	if err != nil {
		return
	}

	return
}

// Reads as many MBAP+modbus frames as necessary until either the response
// matching tt.lastTxnId is received or an error occurs.
func (tt *TCPTransport) ReadResponse() (res *PDU, err error) {
	var txnId uint16

	for {
		// grab a frame
		res, txnId, err = tt.readMBAPFrame(0)

		// ignore unknown protocol identifiers
		if err == ErrUnknownProtocolId {
			continue
		}

		// abort on any other error
		if err != nil {
			return
		}

		// ignore unknown transaction identifiers
		if tt.lastTxnId != txnId {
			tt.logger.Warningf("received unexpected transaction id "+
				"(expected 0x%04x, received 0x%04x)",
				tt.lastTxnId, txnId)
			continue
		}

		break
	}

	return
}

// StreamResponses continuously reads frames from the socket and sends them to the provided channel,
// until the context is cancelled or an error occurs.
func (tt *TCPTransport) StreamResponses(ctx context.Context, data chan<- *PDU) error {
	for {
		deadline := time.Now().Add(tt.timeout)
		if deadline2, ok := ctx.Deadline(); ok {
			// i/o timeouts can fire before the context cancels, add some buffer to avoid that
			deadline2 = deadline2.Add(time.Millisecond)
			if deadline2.Before(deadline) {
				deadline = deadline2
			}
		}
		_ = tt.socket.SetReadDeadline(deadline)
		// grab a frame, this uses a non-standard protocol id
		res, txnId, err := tt.readMBAPFrame(1)

		// ignore unknown protocol identifiers
		if err == ErrUnknownProtocolId {
			continue
		}

		// abort on any other error
		if err != nil {
			ctxErr := ctx.Err()
			if errors.Is(err, os.ErrDeadlineExceeded) && errors.Is(ctxErr, context.DeadlineExceeded) {
				// read deadline was exceeded because context is canceled, ignore this
				return nil
			}
			// TODO: we sometimes get the i/o deadline exceeded before the context is canceled
			return err
		}

		// ignore unknown transaction identifiers
		if txnId != tt.lastTxnId {
			tt.logger.Warningf("received unexpected transaction id "+
				"(expected 0x%04x, received 0x%04x)",
				tt.lastTxnId, txnId)
		}
		tt.lastTxnId = txnId + 1

		select {
		case <-ctx.Done():
			return nil
		case data <- res:
		}
	}
}

// Reads an entire frame (MBAP header + modbus PDU) from the socket.
func (tt *TCPTransport) readMBAPFrame(
	expectedProtocolId uint16,
) (p *PDU, txnId uint16, err error) {
	var rxbuf []byte
	var bytesNeeded int
	var protocolId uint16
	var unitId uint8

	// read the MBAP header
	rxbuf = make([]byte, mbapHeaderLength)
	_, err = io.ReadFull(tt.socket, rxbuf)
	if err != nil {
		return
	}

	// decode the transaction identifier
	txnId = bytesToUint16(BIG_ENDIAN, rxbuf[0:2])
	// decode the protocol identifier
	protocolId = bytesToUint16(BIG_ENDIAN, rxbuf[2:4])
	// store the source unit id
	unitId = rxbuf[6]

	// determine how many more bytes we need to read
	bytesNeeded = int(bytesToUint16(BIG_ENDIAN, rxbuf[4:6]))

	// the byte count includes the unit ID field, which we already have
	bytesNeeded--

	// never read more than the max allowed frame length
	if bytesNeeded+mbapHeaderLength > tt.maxFrameLength {
		err = ErrProtocolError
		return
	}

	// an MBAP length of 0 is illegal
	if bytesNeeded <= 0 {
		err = ErrProtocolError
		return
	}

	// read the PDU
	rxbuf = make([]byte, bytesNeeded)
	_, err = io.ReadFull(tt.socket, rxbuf)
	if err != nil {
		return
	}

	// validate the protocol identifier
	if protocolId != expectedProtocolId {
		err = ErrUnknownProtocolId
		tt.logger.Warningf("received unexpected protocol id 0x%04x", protocolId)
		return
	}

	// store unit id, function code and payload in the PDU object
	p = &PDU{
		UnitId:       unitId,
		FunctionCode: rxbuf[0],
		Payload:      rxbuf[1:],
	}

	return
}

// Turns a PDU into an MBAP frame (MBAP header + PDU) and returns it as bytes.
func (tt *TCPTransport) assembleMBAPFrame(txnId uint16, p *PDU) (payload []byte) {
	// transaction identifier
	payload = uint16ToBytes(BIG_ENDIAN, txnId)
	// protocol identifier (always 0x0000)
	payload = append(payload, 0x00, 0x00)
	// length (covers unit identifier + function code + payload fields)
	payload = append(payload, uint16ToBytes(BIG_ENDIAN, uint16(2+len(p.Payload)))...)
	// unit identifier
	payload = append(payload, p.UnitId)
	// function code
	payload = append(payload, p.FunctionCode)
	// payload
	payload = append(payload, p.Payload...)

	return
}
