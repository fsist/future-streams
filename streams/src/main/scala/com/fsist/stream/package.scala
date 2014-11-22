package com.fsist

import scala.concurrent.Future

/** Overview of types involved in different stages:
  *
  * == Model ==
  *
  * StreamComponent: base trait of everything linked to a StreamBuilder
  * Source: producer. Implementations don't provide a single typed function `produce` to allow specializations to
  *                   avoid the overhead of wrapping every element in Option to indicate EOF. TODO maybe change this?
  * Sink: consumer. Implementations provide the onNext/onComplete/onError triplet.
  * Connector: has >=1 sources and >=1 sinks and routing logic.
  * Transform: a generalized source -> source mapping.
  *
  * == Internal representation in the Builder during modeling ==
  *
  * Every StreamComponent is register()ed and becomes a graph node: this is every Source, Sink, Transform and Connector.
  * Every link is registered and becomes a graph edge. Graph edge origins are Sources, Transforms and Connectors.
  * Edge destinations are Sinks, Transforms and Connectors.
  */
package object stream {
  val futureSuccess = Future.successful(())
}
