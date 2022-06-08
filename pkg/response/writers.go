package response

import (
	"bytes"
	"encoding/json"
	"gopkg.in/yaml.v2"
	"io"
)

func Json(w io.Writer, v interface{}) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}

	out := bytes.NewBuffer(nil)
	if err := json.Indent(out, payload, "", "	"); err != nil {
		return err
	}

	return write(w, out.Bytes())
}

func Yaml(w io.Writer, v interface{}) error {
	payload, err := yaml.Marshal(v)
	if err != nil {
		return err
	}

	return write(w, payload)
}

func write(w io.Writer, data []byte) error {
	data = append(data, '\n')
	if _, err := w.Write(append(data, '\n')); err != nil {
		return err
	}

	return nil
}

func mustWrite(w io.Writer, data []byte) {
	w.Write(append(data, '\n'))
}
