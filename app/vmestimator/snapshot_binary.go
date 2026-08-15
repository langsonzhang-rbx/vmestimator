package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/axiomhq/hyperloglog"
)

// Binary snapshot format:
//
// For each snapshot in the stream:
//   uint32  total snapshot length (excluding this field)
//   --- snapshot body ---
//   int64   Interval (nanoseconds)
//   int64   GroupLimit
//   int64   GroupRejectSize
//   uint32  Filter length + Filter bytes
//   uint32  GroupBy count + GroupBy entries (each: uint32 len + bytes)
//   uint32  Labels count + Labels entries (each: uint32 keyLen + key + uint32 valLen + val)
//   uint32  Sketches count + Sketch entries (each: uint64 key + uint32 sketchLen + sketchBytes)
//
// A total snapshot length of 0 marks end of stream.

var byteOrder = binary.LittleEndian

func writeUint32(w io.Writer, v uint32) error {
	var buf [4]byte
	byteOrder.PutUint32(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func writeUint64(w io.Writer, v uint64) error {
	var buf [8]byte
	byteOrder.PutUint64(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func writeInt64(w io.Writer, v int64) error {
	return writeUint64(w, uint64(v))
}

func writeBytes(w io.Writer, b []byte) error {
	if err := writeUint32(w, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func writeString(w io.Writer, s string) error {
	return writeBytes(w, bytesutil.ToUnsafeBytes(s))
}

func readUint32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return byteOrder.Uint32(buf[:]), nil
}

func readUint64(r io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return byteOrder.Uint64(buf[:]), nil
}

func readInt64(r io.Reader) (int64, error) {
	v, err := readUint64(r)
	return int64(v), err
}

func readBytes(r io.Reader, dst *[]byte) ([]byte, error) {
	n, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	if cap(*dst) < int(n) {
		*dst = make([]byte, n)
	}
	*dst = (*dst)[:n]
	if _, err := io.ReadFull(r, *dst); err != nil {
		return nil, err
	}
	return *dst, nil
}

func readString(r io.Reader, dst *string) (string, error) {
	var dstBytes []byte
	b, err := readBytes(r, &dstBytes)
	if err != nil {
		return "", err
	}
	*dst = bytesutil.ToUnsafeString(b)
	return *dst, nil
}

var snapshotBufPool sync.Pool

func getSnapshotBuf() *bytesWriter {
	if v := snapshotBufPool.Get(); v != nil {
		bw := v.(*bytesWriter)
		bw.buf = bw.buf[:0]
		return bw
	}
	return &bytesWriter{buf: make([]byte, 0, 64*1024)}
}

func putSnapshotBuf(bw *bytesWriter) {
	snapshotBufPool.Put(bw)
}

// encodeSnapshot writes a snapshot in binary format to w.
func encodeSnapshot(w io.Writer, s *snapshot) error {
	bw := getSnapshotBuf()
	defer putSnapshotBuf(bw)

	if err := writeSnapshotBody(bw, s); err != nil {
		return err
	}

	if err := writeUint32(w, uint32(len(bw.buf))); err != nil {
		return err
	}
	_, err := w.Write(bw.buf)
	return err
}

var sketchBufPool sync.Pool

func getSketchBuf() []byte {
	if v := sketchBufPool.Get(); v != nil {
		return v.([]byte)[:0]
	}
	return make([]byte, 0, 4096)
}

func putSketchBuf(buf []byte) {
	sketchBufPool.Put(buf)
}

func writeSnapshotBody(w io.Writer, s *snapshot) error {
	if err := writeInt64(w, int64(s.Interval)); err != nil {
		return err
	}
	if err := writeInt64(w, s.GroupLimit); err != nil {
		return err
	}
	if err := writeInt64(w, s.GroupRejectSize); err != nil {
		return err
	}
	if err := writeString(w, s.Filter); err != nil {
		return err
	}
	if err := writeUint32(w, uint32(len(s.GroupBy))); err != nil {
		return err
	}
	for _, g := range s.GroupBy {
		if err := writeString(w, g); err != nil {
			return err
		}
	}
	if err := writeUint32(w, uint32(len(s.Labels))); err != nil {
		return err
	}
	for k, v := range s.Labels {
		if err := writeString(w, k); err != nil {
			return err
		}
		if err := writeString(w, v); err != nil {
			return err
		}
	}
	if err := writeUint32(w, uint32(len(s.Sketches))); err != nil {
		return err
	}
	for key, ssk := range s.Sketches {
		if err := writeUint64(w, key); err != nil {
			return err
		}
		if err := writeUint32(w, uint32(len(ssk.Values))); err != nil {
			return err
		}
		for _, v := range ssk.Values {
			if err := writeString(w, v); err != nil {
				return err
			}
		}
		sketchBuf := getSketchBuf()
		sketchBytes, err := ssk.Sketch.AppendBinary(sketchBuf)
		if err != nil {
			putSketchBuf(sketchBuf)
			return fmt.Errorf("marshal sketch: %w", err)
		}
		if err := writeBytes(w, sketchBytes); err != nil {
			putSketchBuf(sketchBuf)
			return err
		}
		putSketchBuf(sketchBuf)
	}
	return nil
}

// decodeSnapshotsBinary reads a stream of binary-encoded snapshots from r
// and calls cb for each one.
func decodeSnapshotsBinary(r io.Reader, cb func(s *snapshot)) error {
	s := newSnapshot()
	var dstBytes []byte
	var dstStr string
	for {
		totalLen, err := readUint32(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if totalLen == 0 {
			return nil
		}

		// Read the snapshot body
		body := make([]byte, totalLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return err
		}
		lr := &bytesReader{buf: body}

		s.reset()
		interval, err := readInt64(lr)
		if err != nil {
			return err
		}
		s.Interval = time.Duration(interval)
		s.GroupLimit, err = readInt64(lr)
		if err != nil {
			return err
		}
		s.GroupRejectSize, err = readInt64(lr)
		if err != nil {
			return err
		}
		s.Filter, err = readString(lr, &dstStr)
		if err != nil {
			return err
		}
		groupByCount, err := readUint32(lr)
		if err != nil {
			return err
		}
		s.GroupBy = s.GroupBy[:0]
		for i := uint32(0); i < groupByCount; i++ {
			g, err := readString(lr, &dstStr)
			if err != nil {
				return err
			}
			s.GroupBy = append(s.GroupBy, g)
		}
		labelsCount, err := readUint32(lr)
		if err != nil {
			return err
		}
		if labelsCount > 0 {
			s.Labels = make(map[string]string, labelsCount)
		}
		for i := uint32(0); i < labelsCount; i++ {
			k, err := readString(lr, &dstStr)
			if err != nil {
				return err
			}
			v, err := readString(lr, &dstStr)
			if err != nil {
				return err
			}
			s.Labels[k] = v
		}
		sketchesCount, err := readUint32(lr)
		if err != nil {
			return err
		}
		if sketchesCount > 0 {
			s.Sketches = make(map[uint64]SnapshotSketch, sketchesCount)
		}
		for i := uint32(0); i < sketchesCount; i++ {
			key, err := readUint64(lr)
			if err != nil {
				return err
			}
			valuesCount, err := readUint32(lr)
			if err != nil {
				return err
			}
			var values []string
			for j := uint32(0); j < valuesCount; j++ {
				v, err := readString(lr, &dstStr)
				if err != nil {
					return err
				}
				values = append(values, v)
			}
			sketchBytes, err := readBytes(lr, &dstBytes)
			if err != nil {
				return err
			}
			sk := hyperloglog.New()
			if err := sk.UnmarshalBinary(sketchBytes); err != nil {
				return fmt.Errorf("unmarshal sketch: %w", err)
			}
			s.Sketches[key] = SnapshotSketch{
				Values: values,
				Sketch: sk,
			}
		}

		cb(s)
	}
}

// bytesWriter is a simple io.Writer backed by a byte slice
type bytesWriter struct {
	buf []byte
}

func (w *bytesWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// bytesReader is a simple io.Reader backed by a byte slice
type bytesReader struct {
	buf []byte
	pos int
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.buf) {
		return 0, io.EOF
	}
	n := copy(p, r.buf[r.pos:])
	r.pos += n
	return n, nil
}
