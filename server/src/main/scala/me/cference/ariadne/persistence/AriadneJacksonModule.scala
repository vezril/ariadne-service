package me.cference.ariadne.persistence

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.Module.SetupContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode, SerializerProvider}
import me.cference.ariadne.domain.*
import me.cference.ariadne.domain.price.{PriceEvent, PriceSource}
import me.cference.ariadne.domain.product.{ProductEvent, ProductStatus}
import me.cference.ariadne.domain.purchase.{PurchaseEvent, PurchaseSource}
import me.cference.ariadne.domain.resolution.ResolutionEvent
import me.cference.ariadne.domain.store.StoreEvent

/**
 * Teaches Pekko's Jackson mapper the domain's value types and its polymorphic ADTs. Registered via
 * `pekko.serialization.jackson.jackson-modules` in `serialization.conf`.
 *
 * Two problems are solved here, both discovered by a round-trip test rather than assumed:
 *
 *   1. Scala 3 `enum`s with parameterised cases are NOT Jackson-serialisable — the mapper treats
 *      them as Java enums and fails with "Failed to create Enum instance". The journaled ADTs are
 *      therefore sealed traits, and the mix-ins in `DomainMixins` supply their type information. 2.
 *      The value types have private constructors, because they refuse invalid values. Replay uses
 *      the trusted rehydration factories: these values were validated before they were ever
 *      persisted, and re-validating on replay would let a later rule change retroactively reject
 *      recorded history.
 */
final class AriadneJacksonModule extends SimpleModule("AriadneDomainModule") {

  // --- Money <-> { amount, currency } ------------------------------------
  addSerializer(
    classOf[Money],
    new StdSerializer[Money](classOf[Money]) {
      def serialize(v: Money, gen: JsonGenerator, p: SerializerProvider): Unit = {
        gen.writeStartObject()
        gen.writeStringField("amount", v.amount.toString)
        gen.writeStringField("currency", v.currency.toString)
        gen.writeEndObject()
      }
    }
  )
  addDeserializer(
    classOf[Money],
    new StdDeserializer[Money](classOf[Money]) {
      def deserialize(p: JsonParser, ctx: DeserializationContext): Money = {
        val n: JsonNode = p.getCodec.readTree(p)
        Money.unsafe(BigDecimal(n.get("amount").asText), Currency.valueOf(n.get("currency").asText))
      }
    }
  )

  // --- Quantity <-> { amount, unit } -------------------------------------
  addSerializer(
    classOf[Quantity],
    new StdSerializer[Quantity](classOf[Quantity]) {
      def serialize(v: Quantity, gen: JsonGenerator, p: SerializerProvider): Unit = {
        gen.writeStartObject()
        gen.writeStringField("amount", v.amount.toString)
        gen.writeStringField("unit", v.unit.toString)
        gen.writeEndObject()
      }
    }
  )
  addDeserializer(
    classOf[Quantity],
    new StdDeserializer[Quantity](classOf[Quantity]) {
      def deserialize(p: JsonParser, ctx: DeserializationContext): Quantity = {
        val n: JsonNode = p.getCodec.readTree(p)
        Quantity.unsafe(
          BigDecimal(n.get("amount").asText),
          MeasureUnit.valueOf(n.get("unit").asText)
        )
      }
    }
  )

  // --- UnitPrice <-> { amount, currency, per } ---------------------------
  addSerializer(
    classOf[UnitPrice],
    new StdSerializer[UnitPrice](classOf[UnitPrice]) {
      def serialize(v: UnitPrice, gen: JsonGenerator, p: SerializerProvider): Unit = {
        gen.writeStartObject()
        gen.writeStringField("amount", v.amount.toString)
        gen.writeStringField("currency", v.currency.toString)
        gen.writeStringField("perAmount", v.per.amount.toString)
        gen.writeStringField("perUnit", v.per.unit.toString)
        gen.writeEndObject()
      }
    }
  )
  addDeserializer(
    classOf[UnitPrice],
    new StdDeserializer[UnitPrice](classOf[UnitPrice]) {
      def deserialize(p: JsonParser, ctx: DeserializationContext): UnitPrice = {
        val n: JsonNode = p.getCodec.readTree(p)
        UnitPrice.rehydrate(
          BigDecimal(n.get("amount").asText),
          Currency.valueOf(n.get("currency").asText),
          Quantity.unsafe(
            BigDecimal(n.get("perAmount").asText),
            MeasureUnit.valueOf(n.get("perUnit").asText)
          )
        )
      }
    }
  )

  override def setupModule(context: SetupContext): Unit = {
    super.setupModule(context)
    context.setMixInAnnotations(classOf[ProductEvent], classOf[DomainMixins.ProductEventMixin])
    context.setMixInAnnotations(classOf[StoreEvent], classOf[DomainMixins.StoreEventMixin])
    context.setMixInAnnotations(classOf[PriceEvent], classOf[DomainMixins.PriceEventMixin])
    context.setMixInAnnotations(classOf[PurchaseEvent], classOf[DomainMixins.PurchaseEventMixin])
    context.setMixInAnnotations(classOf[Origin], classOf[DomainMixins.OriginMixin])
    context.setMixInAnnotations(classOf[ProductStatus], classOf[DomainMixins.ProductStatusMixin])
    context.setMixInAnnotations(classOf[PriceScope], classOf[DomainMixins.PriceScopeMixin])
    context.setMixInAnnotations(classOf[PriceSource], classOf[DomainMixins.PriceSourceMixin])
    context.setMixInAnnotations(classOf[PurchaseSource], classOf[DomainMixins.PurchaseSourceMixin])
    context.setMixInAnnotations(
      classOf[ResolutionEvent],
      classOf[DomainMixins.ResolutionEventMixin]
    )
  }
}
