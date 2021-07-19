package stats

import ()

type TpsStatic struct {
	Tps_20X     int64
	Tps_304     int64
	Tps_3XX     int64
	Tps_404     int64
	Tps_4XX     int64
	Tps_5XX     int64
	Traffic_20X int64
}

func NewTpsStatic() *TpsStatic {
	return &TpsStatic{Tps_20X: int64(0), Tps_304: int64(0), Tps_3XX: int64(0), Tps_404: int64(0), Tps_4XX: int64(0), Tps_5XX: int64(0)}
}

type TpsValue struct {
	status     int
	bodyLength int64
}

func NewTpsValue(status int, bodyLength int64) *TpsValue {
	return &TpsValue{status: status, bodyLength: bodyLength}
}

func (ts *TpsStatic) Add(tv *TpsValue) {

	switch tv.status {
	case 304:
		ts.Tps_304++
		return
	case 404:
		ts.Tps_404++
		return
	}

	header := tv.status / 100
	switch header {
	case 2:
		ts.Tps_20X++
		ts.Traffic_20X += tv.bodyLength
	case 3:
		ts.Tps_3XX++
	case 4:
		ts.Tps_4XX++
	case 5:
		ts.Tps_5XX++
	}
}

func (ts *TpsStatic) ReInit() {
	ts.Tps_20X = int64(0)
	ts.Tps_304 = int64(0)
	ts.Tps_3XX = int64(0)
	ts.Tps_404 = int64(0)
	ts.Tps_4XX = int64(0)
	ts.Tps_5XX = int64(0)
	ts.Traffic_20X = int64(0)
}
