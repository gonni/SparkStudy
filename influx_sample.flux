http://localhost:8086/api/v2/query
application/vnd.flux
application/csv

from(bucket:"hell/autogen")
  |> range(start: -100d)
  |> filter(fn: (r)=>
  	r._measurement == "rtf")
  |> group(columns: ["seedId"], mode: "by")




