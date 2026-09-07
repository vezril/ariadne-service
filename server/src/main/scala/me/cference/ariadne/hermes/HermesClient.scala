package me.cference.ariadne.hermes

import me.cference.hermesmq.grpc.{
  CreateTopicRequest,
  PublishRequest,
  PubSubServiceClient,
  TopicAdminServiceClient
}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.grpc.GrpcClientSettings
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
 * Thin seam over the shared HermesMQ stubs (`lexicon-hermes-grpc`). Mirrors artemis-service's
 * client, which is the fleet's working precedent for talking to this broker.
 *
 * Carries no catalog logic — mapping domain events to messages is `CatalogMessages`, and deciding
 * WHEN to publish is the projection. This knows how to reach the broker and nothing else.
 *
 * App-lifetime singleton: the gRPC channel registers with the system's `CoordinatedShutdown`, so it
 * is released on termination. Do not construct one per publish.
 */
trait CatalogPublisher {
  def publish(msg: CatalogMessages.Outgoing): Future[Unit]
}

final class HermesClient(config: HermesConfig)(using system: ActorSystem[?])
    extends CatalogPublisher {

  private given ExecutionContext = system.executionContext
  private val log = LoggerFactory.getLogger(getClass)

  private val settings: GrpcClientSettings =
    GrpcClientSettings.connectToServiceAt(config.host, config.port)(system).withTls(config.tls)

  private val pubsub: PubSubServiceClient = PubSubServiceClient(settings)(system)
  private val topics: TopicAdminServiceClient = TopicAdminServiceClient(settings)(system)

  def publish(msg: CatalogMessages.Outgoing): Future[Unit] =
    pubsub
      .publish(
        PublishRequest(
          topicId = msg.topic,
          payload = com.google.protobuf.ByteString.copyFromUtf8(msg.payload),
          attributes = msg.attributes,
          // The deterministic {persistenceId}:{seqNr} (§5). The publisher is
          // at-least-once by construction — the offset advances only after a
          // successful publish — so a crash in that window republishes. Handing the
          // broker an idempotency key turns that duplicate into a no-op at the
          // SOURCE rather than making every consumer in the constellation responsible
          // for detecting it.
          idempotencyKey = msg.messageId,
          producerId = HermesClient.ProducerId,
          correlationId = msg.correlationId
        )
      )
      .map(_ => ())

  /**
   * Create the catalog topics, treating already-exists as success (§5).
   *
   * Self-provisioned at startup and never hand-created in the cluster, per the playbook: a topic
   * that exists only because someone ran a command once is a topic that vanishes on the next
   * cluster rebuild.
   *
   * A failure here does NOT stop the service. Ariadne's job is recording facts; publishing is a
   * downstream courtesy, and refusing to boot because a broker is having a bad morning would turn
   * Hermes into a hard dependency of the scraper and the REST surface, which it is not.
   */
  def provisionTopics(names: List[String]): Future[Unit] =
    Future
      .traverse(names) { name =>
        topics
          .createTopic(CreateTopicRequest(topicId = name, labels = Map("owner" -> "ariadne")))
          .map(_ => log.info("Hermes topic ready: {}", name))
          .recover {
            // Already-exists is the expected steady-state answer, not an error: every
            // boot after the first takes this path.
            case NonFatal(e) =>
              log.info(
                "Hermes topic {} not created ({}) — assuming it already exists",
                name,
                e.getMessage
              )
          }
      }
      .map(_ => ())
}

object HermesClient {

  /** Ariadne's identity on the bus — the active-producer metric and log MDC key it. */
  val ProducerId = "ariadne"
}
