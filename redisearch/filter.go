package redisearch

//  Common filter
type Filter struct {
	Field   string
	Options interface{}
}

// FilterExpression the results to a given radius from lon and lat. Radius is given as a number and units
type GeoFilterOptions struct {
	Lon    float64
	Lat    float64
	Radius float64
	Unit   Unit
}

// Limit results to those having numeric values ranging between min and max. min and max follow ZRANGE syntax, and can be -inf, +inf
type NumericFilterOptions struct {
	Min          float64
	ExclusiveMin bool
	Max          float64
	ExclusiveMax bool
}

// units of Radius
type Unit string

const (
	KILOMETERS Unit = "km"
	METERS     Unit = "m"
	FEET       Unit = "ft"
	MILES      Unit = "mi"
)
