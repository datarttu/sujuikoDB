# Glossary

## References

See for example [Jore 4 glossary](https://github.com/HSLdevcom/jore4/blob/main/wiki/glossary.md).
Note that the terms are not fully compatible with Jore4 or other systems, though, as we might simplify or otherwise adjust them to sujuiko-spesific needs.

## Terminology in alphabetical order

#### Analysis segment

> A continuous path of directed links for local analyses and calculations.

- In Finnish: _Analyysiosuus_, _Analyysisegmentti_
- An analysis segment groups together multiple links so they can be treated as one link in various analyses and calculations.
- Adjacent links belonging to the same analysis segments can be mapped from 2D into 1D space, e.g., into the x-axis of a time-space diagram.
- Because of the above, an analysis segment shall not have gaps between the links or branches.

#### Analysis segment link

> A network link belonging to an analysis segment.

- In Finnish: _Analyysiosuuden linkki_, _Analyysisegmentin linkki_, _Analyysilinkki_

#### Journey

> A realized transit service on a route version, planned operating day and initial departure time, driven by a unique vehicle.

- In Finnish: _Toteutunut lähtö_, _Toteutunut vuoro_
- A journey connects HFP observations from the same scheduled trip and unique vehicle into a single path.
- Note: a `trip` would be a planned and scheduled (but not necessarily realized) journey.
- There can be multiple journeys, i.e. realizations, of a single scheduled trip, if multiple vehicles have signed in on the trip on purpose or by accident.

#### Link

> TBD

- In Finnish: _Linkki_
- Links are distinguished by unique link id.
- A link shall start from and end to a node.
- There can be multiple links between the same nodes, but they shall have different link ids and geometries.
- A link can be oneway or two-way.
A oneway link can only be traversed to the direction it has been digitized in.
- A two-way link can be represented by two oneway links that look identical, but one of them has a negative link id and reversed LINESTRING geometry.

#### Node

> TBD

- In Finnish: _Solmu_

#### Route version

> A unique directed path on the network following an ordered list of links and corresponding stops, representing passenger service under a headsign.

- In Finnish: _Reitti_ (in fact, _Reitinsuunta_)
- A route version always has a route id, direction id, version id and a validity date range.
- Multiple route versions can be grouped together by route id and direction id.
- Multiple routes belonging to the same "service" from passenger's point of view can be grouped together by the route id.
- Direction id (1 / 2) expresses the two opposite directions that usually belong to the service and, in most cases, look almost the same on the network map.
- Route versioning means in practice that there has been changes to the route stops, their locations, and/or route links in the past, in which case a route with a new _version id_ is created.

#### Route link

> A strictly oriented representation of a network link with a sequence number belonging to a route.

- In Finnish: _Reitinlinkki_
- A route link connects a _route_ to a network _link_ that the route uses, and it has a sequence number.
This way a solid LINESTRING geometry can be created for the route by merging the route links ordered by the sequence number.
- While links can be one- or two-way, the route link representation must be strictly one-way to the direction that the route uses the link.

#### Route stop

> A stop that is active for passenger service on a route.

- In Finnish: _Reitinpysäkki_
- A route stop connects a _route_ to a network _stop_ that the route uses for passenger service, and it has a sequence number.
- If all the route stops have link references and there are no errors or gaps in the network, the ordered set of route stops can be used as a basis for constructing an ordered set of _route links_.
- A route may pass through many other stops on its route links on the network too, so it is important that we can distinguish (e.g. in visualizations) between stops along the route in general and stops actually used by the route.

#### Stop version

> A location in the network where transit vehicles stop for passenger service.

- In Finnish: _Pysäkki_
- A stop always has a unique stop id, POINT geometry, version id and a validity date range.
- There can changes to the stop attributes in the past, e.g. the stop location may have been altered.
In that case, there are multiple _versions_ of the same stop id, each with their own non-overlapping validity date ranges and version ids.
- _Stop coverage_ on the network means the section(s) on link(s) where vehicles are expected to have stopped for service: a point would not be enough to model this.
Stop coverage is expressed by a reference to one or multiple _links_ and distance ranges that the stop covers of each link.

#### Vehicle mode

> TBD

- Sujuiko model is focused on street transit modes.
Therefore, possible values for vehicle mode are `bus` and `tram`.
