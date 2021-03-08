# Glossary

## References

See for example [Jore 4 glossary](https://github.com/HSLdevcom/jore4/blob/main/wiki/glossary.md).
Note that the terms are not fully compatible with Jore4 or other systems, though, as we might simplify or otherwise adjust them to sujuiko-spesific needs.

## Terminology in alphabetical order

#### Route

> A single directed path on the network following an ordered list of links, representing passenger service under a headsign.

- In Finnish: _Reitti_ (in fact, _Reitinsuunta_)
- A route always has a route id, direction id, version id and a validity date range.
- Multiple routes belonging to the same "service" from passenger's point of view can be grouped together by the route id.
- Direction id (1 / 2) expresses the two opposite directions that usually belong to the service and, in most cases, look almost the same on the network map.
- Route versioning means in practice that there may have been changes to the route stops and/or route links in history, in which case a route with a new _version id_ is created with non-overlapping validity date range.

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

#### Stop

> A location in the network where transit vehicles stop for passenger service.

- In Finnish: _Pysäkki_
- A stop always has a unique stop id, POINT geometry, version id and a validity date range.
- There can changes to the stop attributes in the past, e.g. the stop location may have been altered.
In that case, there are multiple _versions_ of the same stop id, each with their own non-overlapping validity date ranges and version ids.
- _Stop coverage_ on the network means the section(s) on link(s) where vehicles are expected to have stopped for service: a point would not be enough to model this.
Stop coverage is expressed by a reference to one or multiple _links_ and distance ranges that the stop covers of each link.
