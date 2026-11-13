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
          // successful publish — so a crash in that window republishes.
          //
          // This key is BEST-EFFORT and does not make the publish idempotent.
          // Corrected by the HermesMQ session 2026-11-13: `HERMESMQ_DEDUP_WINDOW`
          // defaults to 0s, i.e. dedup OFF, and with it off the key is accepted and
          // ignored — every republish becomes a second delivery and nothing errors.
          // Even with a window configured it is a WINDOW: the seen-key set is pruned
          // to roughly one window of traffic, so an outbox replaying from an old
          // offset (restore, offset reset, operator action) — the exact case a
          // deterministic key is designed for — falls outside it.
          //
          // So it is sent because it costs nothing and does cover the retry-within-
          // seconds case, NOT because it relieves consumers. Consumers must dedup;
          // §5 says so and names the mechanism.
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
          .map(_ => log.info("Hermes topic created: {}", name))
          .recover {
            // ALREADY_EXISTS is the expected steady-state answer — every boot after the
            // first takes this path — but it is NOT unambiguous, and the ambiguity is
            // unrecoverable rather than cosmetic.
            //
            // Flagged by the HermesMQ session 2026-11-13: `Topic.decide` rejects
            // CreateTopic on `state.created`, not `state.exists`, so a SOFT-DELETED topic
            // id is poisoned permanently by design (its journal still exists, and
            // re-creating would resurrect old outstanding messages). A deleted
            // `product.*` id therefore answers ALREADY_EXISTS forever while every publish
            // to it fails — startup would look healthy indefinitely.
            //
            // Logged distinctly so that case is greppable instead of hiding inside a
            // routine line. We cannot tell the two apart from here; the broker does not
            // offer a way to, and guessing would be worse than naming the ambiguity.
            case e: io.grpc.StatusRuntimeException
                if e.getStatus.getCode == io.grpc.Status.Code.ALREADY_EXISTS =>
              log.info(
                "Hermes topic {} already exists (steady state) — note this answer is also " +
                  "what a soft-DELETED topic id returns, permanently; if publishes to it fail, " +
                  "the id is poisoned and needs a new name",
                name
              )
            // Anything else is a real failure and must not read as routine. This branch
            // used to log at INFO saying "assuming it already exists", so an unreachable
            // broker or a rejected credential looked identical to the happy path.
            case NonFatal(e) =>
              log.warn(
                "Hermes topic {} could not be provisioned ({}) — publishes to it will fail",
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
