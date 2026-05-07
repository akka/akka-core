/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson

import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

import scala.annotation.nowarn
import scala.collection.immutable
import scala.jdk.OptionConverters._
import scala.util.Failure
import scala.util.Success

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonFactoryBuilder
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.core.StreamWriteFeature
import com.fasterxml.jackson.core.json.JsonReadFeature
import com.fasterxml.jackson.core.json.JsonWriteFeature
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import com.typesafe.config.Config
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.DynamicAccess
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.setup.Setup
import akka.annotation.InternalStableApi
import akka.event.Logging
import akka.event.LoggingAdapter

import com.fasterxml.jackson.databind.cfg.ConstructorDetector

object JacksonObjectMapperProvider extends ExtensionId[JacksonObjectMapperProvider] with ExtensionIdProvider {
  override def get(system: ActorSystem): JacksonObjectMapperProvider = super.get(system)
  override def get(system: ClassicActorSystemProvider): JacksonObjectMapperProvider = super.get(system)

  override def lookup = JacksonObjectMapperProvider

  override def createExtension(system: ExtendedActorSystem): JacksonObjectMapperProvider =
    new JacksonObjectMapperProvider(system)

  /**
   * The configuration for a given `bindingName`.
   */
  def configForBinding(bindingName: String, systemConfig: Config): Config = {
    val basePath = "akka.serialization.jackson"
    val baseConf = systemConfig.getConfig("akka.serialization.jackson")
    if (systemConfig.hasPath(s"$basePath.$bindingName"))
      systemConfig.getConfig(s"$basePath.$bindingName").withFallback(baseConf)
    else
      baseConf
  }

  private def createJsonFactory(
      bindingName: String,
      objectMapperFactory: JacksonObjectMapperFactory,
      config: Config,
      baseJsonFactory: Option[JsonFactory]): JsonFactory = {

    val jsonFactory: JsonFactory = baseJsonFactory match {
      case Some(factory) =>
        // Issue #28918 not possible to use new JsonFactoryBuilder(jsonFactory) here.
        // It doesn't preserve the formatParserFeatures and formatGeneratorFeatures in
        // CBORFactor. Therefore we use JsonFactory and configure the features with mappedFeature
        // instead of using JsonFactoryBuilder (new in Jackson 2.10.0).
        factory
      case None =>
        new JsonFactoryBuilder().build()
    }

    val configuredStreamReadFeatures =
      features(config, "stream-read-features").map {
        case (enumName, value) => StreamReadFeature.valueOf(enumName) -> value
      }
    val streamReadFeatures =
      objectMapperFactory.overrideConfiguredStreamReadFeatures(bindingName, configuredStreamReadFeatures)
    streamReadFeatures.foreach {
      case (feature, value) => jsonFactory.configure(feature.mappedFeature, value)
    }

    val configuredStreamWriteFeatures =
      features(config, "stream-write-features").map {
        case (enumName, value) => StreamWriteFeature.valueOf(enumName) -> value
      }
    val streamWriteFeatures =
      objectMapperFactory.overrideConfiguredStreamWriteFeatures(bindingName, configuredStreamWriteFeatures)
    streamWriteFeatures.foreach {
      case (feature, value) => jsonFactory.configure(feature.mappedFeature, value)
    }

    val configuredJsonReadFeatures =
      features(config, "json-read-features").map {
        case (enumName, value) => JsonReadFeature.valueOf(enumName) -> value
      }
    val jsonReadFeatures =
      objectMapperFactory.overrideConfiguredJsonReadFeatures(bindingName, configuredJsonReadFeatures)
    jsonReadFeatures.foreach {
      case (feature, value) => jsonFactory.configure(feature.mappedFeature, value)
    }

    val configuredJsonWriteFeatures =
      features(config, "json-write-features").map {
        case (enumName, value) => JsonWriteFeature.valueOf(enumName) -> value
      }
    val jsonWriteFeatures =
      objectMapperFactory.overrideConfiguredJsonWriteFeatures(bindingName, configuredJsonWriteFeatures)
    jsonWriteFeatures.foreach {
      case (feature, value) => jsonFactory.configure(feature.mappedFeature, value)
    }

    jsonFactory
  }

  @nowarn("msg=deprecated")
  private def configureObjectMapperFeatures(
      bindingName: String,
      objectMapper: ObjectMapper,
      objectMapperFactory: JacksonObjectMapperFactory,
      config: Config): Unit = {

    val configuredSerializationFeatures =
      features(config, "serialization-features").map {
        case (enumName, value) => SerializationFeature.valueOf(enumName) -> value
      }
    val serializationFeatures =
      objectMapperFactory.overrideConfiguredSerializationFeatures(bindingName, configuredSerializationFeatures)
    serializationFeatures.foreach {
      case (feature, value) => objectMapper.configure(feature, value)
    }

    val configuredDeserializationFeatures =
      features(config, "deserialization-features").map {
        case (enumName, value) => DeserializationFeature.valueOf(enumName) -> value
      }
    val deserializationFeatures =
      objectMapperFactory.overrideConfiguredDeserializationFeatures(bindingName, configuredDeserializationFeatures)
    deserializationFeatures.foreach {
      case (feature, value) => objectMapper.configure(feature, value)
    }

    val configuredMapperFeatures = features(config, "mapper-features").map {
      case (enumName, value) => MapperFeature.valueOf(enumName) -> value
    }
    val mapperFeatures = objectMapperFactory.overrideConfiguredMapperFeatures(bindingName, configuredMapperFeatures)

    mapperFeatures.foreach {
      case (feature, value) =>
        // TODO: This is deprecated and should used JsonMapper.Builder, but that would be difficult without
        // breaking compatibility for custom JacksonObjectMapperProvider that may create a custom instance
        // of the ObjectMapper
        objectMapper.configure(feature, value)
    }

    val constructorDetector = config.getString("constructor-detector-mode") match {
      case "USE_PROPERTIES_BASED" => ConstructorDetector.USE_PROPERTIES_BASED
      case "USE_DELEGATING"       => ConstructorDetector.USE_DELEGATING
      case "DEFAULT"              => ConstructorDetector.DEFAULT
      case "EXPLICIT_ONLY"        => ConstructorDetector.EXPLICIT_ONLY
      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown constructor-detector-mode [$unknown], must be one of [USE_PROPERTIES_BASED, USE_DELEGATING, DEFAULT, EXPLICIT_ONLY]")
    }
    objectMapper.setConstructorDetector(constructorDetector)

    val configuredJsonParserFeatures = features(config, "json-parser-features").map {
      case (enumName, value) => JsonParser.Feature.valueOf(enumName) -> value
    }
    val jsonParserFeatures =
      objectMapperFactory.overrideConfiguredJsonParserFeatures(bindingName, configuredJsonParserFeatures)
    jsonParserFeatures.foreach {
      case (feature, value) => objectMapper.configure(feature, value)
    }

    val configuredJsonGeneratorFeatures = features(config, "json-generator-features").map {
      case (enumName, value) => JsonGenerator.Feature.valueOf(enumName) -> value
    }
    val jsonGeneratorFeatures =
      objectMapperFactory.overrideConfiguredJsonGeneratorFeatures(bindingName, configuredJsonGeneratorFeatures)
    jsonGeneratorFeatures.foreach {
      case (feature, value) => objectMapper.configure(feature, value)
    }
  }

  private def configureObjectVisibility(
      bindingName: String,
      objectMapper: ObjectMapper,
      objectMapperFactory: JacksonObjectMapperFactory,
      config: Config): Unit = {

    val configuredVisibility: immutable.Seq[(PropertyAccessor, JsonAutoDetect.Visibility)] =
      configPairs(config, "visibility").map {
        case (property, visibility) =>
          PropertyAccessor.valueOf(property) -> JsonAutoDetect.Visibility.valueOf(visibility)
      }
    val visibility =
      objectMapperFactory.overrideConfiguredVisibility(bindingName, configuredVisibility)
    visibility.foreach {
      case (property, visibility) => objectMapper.setVisibility(property, visibility)
    }

  }

  private def configureObjectMapperModules(
      bindingName: String,
      objectMapper: ObjectMapper,
      objectMapperFactory: JacksonObjectMapperFactory,
      config: Config,
      dynamicAccess: DynamicAccess,
      log: Option[LoggingAdapter]): Unit = {

    import scala.jdk.CollectionConverters._

    val configuredModules = config.getStringList("jackson-modules").asScala
    val modules1 =
      configuredModules.flatMap { fqcn =>
        if (isModuleEnabled(fqcn, dynamicAccess)) {
          dynamicAccess.createInstanceFor[Module](fqcn, Nil) match {
            case Success(m) => Some(m)
            case Failure(e) =>
              log.foreach(
                _.error(
                  e,
                  s"Could not load configured Jackson module [$fqcn], " +
                  "please verify classpath dependencies or amend the configuration " +
                  "[akka.serialization.jackson-modules]. Continuing without this module."))
              None
          }
        } else
          None
      }

    val modules2 = modules1.map { module =>
      if (module.isInstanceOf[ParameterNamesModule])
        // ParameterNamesModule needs a special case for the constructor to ensure that single-parameter
        // constructors are handled the same way as constructors with multiple parameters.
        // See https://github.com/FasterXML/jackson-module-parameter-names#delegating-creator
        new ParameterNamesModule(JsonCreator.Mode.PROPERTIES)
      else module
    }.toList

    val modules3 = objectMapperFactory.overrideConfiguredModules(bindingName, modules2)

    modules3.foreach { module =>
      objectMapper.registerModule(module)
      log.foreach(_.debug("Registered Jackson module [{}]", module.getClass.getName))
    }
  }

  /**
   * INTERNAL API: Use [[JacksonObjectMapperProvider#create]]
   *
   * This is needed by one test in Lagom where the ObjectMapper is created without starting and ActorSystem.
   */
  @InternalStableApi
  def createObjectMapper(
      bindingName: String,
      jsonFactory: Option[JsonFactory],
      objectMapperFactory: JacksonObjectMapperFactory,
      config: Config,
      dynamicAccess: DynamicAccess,
      log: Option[LoggingAdapter]): ObjectMapper = {

    val configuredJsonFactory = createJsonFactory(bindingName, objectMapperFactory, config, jsonFactory)
    val mapper = objectMapperFactory.newObjectMapper(bindingName, configuredJsonFactory)

    configureObjectMapperFeatures(bindingName, mapper, objectMapperFactory, config)
    configureObjectMapperModules(bindingName, mapper, objectMapperFactory, config, dynamicAccess, log)
    configureObjectVisibility(bindingName, mapper, objectMapperFactory, config)

    mapper
  }

  private def isModuleEnabled(fqcn: String, dynamicAccess: DynamicAccess): Boolean =
    fqcn match {
      case "akka.serialization.jackson.AkkaTypedJacksonModule" =>
        // akka-actor-typed dependency is "provided" and may not be included
        dynamicAccess.classIsOnClasspath("akka.actor.typed.ActorRef")
      case "akka.serialization.jackson.AkkaStreamJacksonModule" =>
        // akka-stream dependency is "provided" and may not be included
        dynamicAccess.classIsOnClasspath("akka.stream.Graph")
      case _ => true
    }

  private def features(config: Config, section: String): immutable.Seq[(String, Boolean)] = {
    import scala.jdk.CollectionConverters._
    val cfg = config.getConfig(section)
    cfg.root.keySet().asScala.map(key => key -> cfg.getBoolean(key)).toList
  }

  private def configPairs(config: Config, section: String): immutable.Seq[(String, String)] = {
    import scala.jdk.CollectionConverters._
    val cfg = config.getConfig(section)
    cfg.root.keySet().asScala.map(key => key -> cfg.getString(key)).toList
  }
}

/**
 * Registry of shared `ObjectMapper` instances, each with it's unique `bindingName`.
 */
final class JacksonObjectMapperProvider(system: ExtendedActorSystem) extends Extension {
  private val objectMappers = new ConcurrentHashMap[String, ObjectMapper]

  /**
   * Scala API: Returns an existing Jackson `ObjectMapper` that was created previously with this method, or
   * creates a new instance.
   *
   * The `ObjectMapper` is created with sensible defaults and modules configured
   * in `akka.serialization.jackson.jackson-modules`. It's using [[JacksonObjectMapperProviderSetup]]
   * if the `ActorSystem` is started with such [[akka.actor.setup.ActorSystemSetup]].
   *
   * The returned `ObjectMapper` must not be modified, because it may already be in use and such
   * modifications are not thread-safe.
   *
   * @param bindingName name of this `ObjectMapper`
   * @param jsonFactory optional `JsonFactory` such as `CBORFactory`, for plain JSON `None` (defaults)
   *                    can be used
   */
  def getOrCreate(bindingName: String, jsonFactory: Option[JsonFactory]): ObjectMapper = {
    objectMappers.computeIfAbsent(bindingName, _ => create(bindingName, jsonFactory))
  }

  /**
   * Java API: Returns an existing Jackson `ObjectMapper` that was created previously with this method, or
   * creates a new instance.
   *
   * The `ObjectMapper` is created with sensible defaults and modules configured
   * in `akka.serialization.jackson.jackson-modules`. It's using [[JacksonObjectMapperProviderSetup]]
   * if the `ActorSystem` is started with such [[akka.actor.setup.ActorSystemSetup]].
   *
   * The returned `ObjectMapper` must not be modified, because it may already be in use and such
   * modifications are not thread-safe.
   *
   * @param bindingName name of this `ObjectMapper`
   * @param jsonFactory optional `JsonFactory` such as `CBORFactory`, for plain JSON `None` (defaults)
   *                    can be used
   */
  def getOrCreate(bindingName: String, jsonFactory: Optional[JsonFactory]): ObjectMapper =
    getOrCreate(bindingName, jsonFactory.toScala)

  /**
   * Scala API: Creates a new instance of a Jackson `ObjectMapper` with sensible defaults and modules configured
   * in `akka.serialization.jackson.jackson-modules`. It's using [[JacksonObjectMapperProviderSetup]]
   * if the `ActorSystem` is started with such [[akka.actor.setup.ActorSystemSetup]].
   *
   * @param bindingName name of this `ObjectMapper`
   * @param jsonFactory optional `JsonFactory` such as `CBORFactory`, for plain JSON `None` (defaults)
   *                    can be used
   * @see [[JacksonObjectMapperProvider#getOrCreate]]
   */
  def create(bindingName: String, jsonFactory: Option[JsonFactory]): ObjectMapper = {
    val log = Logging.getLogger(system, JacksonObjectMapperProvider.getClass)
    val dynamicAccess = system.dynamicAccess

    val config = JacksonObjectMapperProvider.configForBinding(bindingName, system.settings.config)

    val factory = system.settings.setup.get[JacksonObjectMapperProviderSetup] match {
      case Some(setup) => setup.factory
      case None        => new JacksonObjectMapperFactory // default
    }

    JacksonObjectMapperProvider.createObjectMapper(bindingName, jsonFactory, factory, config, dynamicAccess, Some(log))
  }

  /**
   * Java API: Creates a new instance of a Jackson `ObjectMapper` with sensible defaults and modules configured
   * in `akka.serialization.jackson.jackson-modules`. It's using [[JacksonObjectMapperProviderSetup]]
   * if the `ActorSystem` is started with such [[akka.actor.setup.ActorSystemSetup]].
   *
   * @param bindingName name of this `ObjectMapper`
   * @param jsonFactory optional `JsonFactory` such as `CBORFactory`, for plain JSON `None` (defaults)
   *                    can be used
   * @see [[JacksonObjectMapperProvider#getOrCreate]]
   */
  def create(bindingName: String, jsonFactory: Optional[JsonFactory]): ObjectMapper =
    create(bindingName, jsonFactory.toScala)

}

object JacksonObjectMapperProviderSetup {

  /**
   * Scala API: factory for defining a `JacksonObjectMapperProvider` that is passed in when ActorSystem
   * is created rather than creating one from configured class name.
   */
  def apply(factory: JacksonObjectMapperFactory): JacksonObjectMapperProviderSetup =
    new JacksonObjectMapperProviderSetup(factory)

  /**
   * Java API: factory for defining a `JacksonObjectMapperProvider` that is passed in when ActorSystem
   * is created rather than creating one from configured class name.
   */
  def create(factory: JacksonObjectMapperFactory): JacksonObjectMapperProviderSetup =
    apply(factory)

}

/**
 * Setup for defining a `JacksonObjectMapperProvider` that can be passed in when ActorSystem
 * is created rather than creating one from configured class name. Create a subclass of
 * [[JacksonObjectMapperFactory]] and override the methods to amend the defaults.
 */
final class JacksonObjectMapperProviderSetup(val factory: JacksonObjectMapperFactory) extends Setup

/**
 * Used with [[JacksonObjectMapperProviderSetup]] for defining a `JacksonObjectMapperProvider` that can be
 * passed in when ActorSystem is created rather than creating one from configured class name.
 * Create a subclass and override the methods to amend the defaults.
 */
class JacksonObjectMapperFactory {

  /**
   * Override this method to create a new custom instance of `ObjectMapper` for the given `serializerIdentifier`.
   *
   * @param bindingName name of this `ObjectMapper`
   * @param jsonFactory optional `JsonFactory` such as `CBORFactory`, for plain JSON `None` (defaults)
   *                    can be used
   */
  def newObjectMapper(@nowarn("msg=never used") bindingName: String, jsonFactory: JsonFactory): ObjectMapper =
    JsonMapper.builder(jsonFactory).build()

  /**
   * After construction of the `ObjectMapper` the configured modules are added to
   * the mapper. These modules can be amended programatically by overriding this method and
   * return the modules that are to be applied to the `ObjectMapper`.
   *
   * When implementing a `JacksonObjectMapperFactory` with Java the `immutable.Seq` can be
   * created with [[akka.japi.Util.immutableSeq]].
   *
   * @param bindingName bindingName name of this `ObjectMapper`
   * @param configuredModules the list of `Modules` that were configured in
   *                           `akka.serialization.jackson.deserialization-features`
   */
  def overrideConfiguredModules(
      @nowarn("msg=never used") bindingName: String,
      configuredModules: immutable.Seq[Module]): immutable.Seq[Module] =
    configuredModules

  /**
   * After construction of the `ObjectMapper` the configured serialization features are applied to
   * the mapper. These features can be amended programatically by overriding this method and
   * return the features that are to be applied to the `ObjectMapper`.
   *
   * When implementing a `JacksonObjectMapperFactory` with Java the `immutable.Seq` can be
   * created with [[akka.japi.Util.immutableSeq]].
   *
   * @param bindingName bindingName name of this `ObjectMapper`
   * @param configuredFeatures the list of `SerializationFeature` that were configured in
   *                           `akka.serialization.jackson.serialization-features`
   */
  def overrideConfiguredSerializationFeatures(
      @nowarn("msg=never used") bindingName: String,
      configuredFeatures: immutable.Seq[(SerializationFeature, Boolean)])
      : immutable.Seq[(SerializationFeature, Boolean)] =
    configuredFeatures

  /**
   * After construction of the `ObjectMapper` the configured deserialization features are applied to
   * the mapper. These features can be amended programatically by overriding this method and
   * return the features that are to be applied to the `ObjectMapper`.
   *
   * When implementing a `JacksonObjectMapperFactory` with Java the `immutable.Seq` can be
   * created with [[akka.japi.Util.immutableSeq]].
   *
   * @param bindingName bindingName name of this `ObjectMapper`
   * @param configuredFeatures the list of `DeserializationFeature` that were configured in
   *                           `akka.serialization.jackson.deserialization-features`
   */
  def overrideConfiguredDeserializationFeatures(
      @nowarn("msg=never used") bindingName: String,
      configuredFeatures: immutable.Seq[(DeserializationFeature, Boolean)])
      : immutable.Seq[(DeserializationFeature, Boolean)] =
    configuredFeatures

  /**
   * After construction of the `ObjectMapper` the configured mapper features are applied to
   * the mapper. These features can be amended programmatically by overriding this method and
   * return the features that are to be applied to the `ObjectMapper`.
   *
   * @param bindingName bindingName name of this `ObjectMapper`
   * @param configuredFeatures the list of `MapperFeatures` that were configured in `akka.serialization.jackson.mapper-features`
   */
  def overrideConfiguredMapperFeatures(
      @nowarn("msg=never used") bindingName: String,
      configuredFeatures: immutable.Seq[(MapperFeature, Boolean)]): immutable.Seq[(MapperFeature, Boolean)] =
    configuredFeatures

  /**
   * After construction of the `ObjectMapper` the configured JSON parser features are applied to
   * the mapper. These features can be amended programmatically by overriding this method and
   * return the features that are to be applied to the `ObjectMapper`.
   *
   * @param bindingName bindingName name of this `ObjectMapper`
   * @param configuredFeatures the list of `JsonParser.Feature` that were configured in `akka.serialization.jackson.json-parser-features`
   */
  def overrideConfiguredJsonParserFeatures(
      @nowarn("msg=never used") bindingName: String,
      configuredFeatures: immutable.Seq[(JsonParser.Feature, Boolean)]): immutable.Seq[(JsonParser.Feature, Boolean)] =
    configuredFeatures

  /**
   * After construction of the `ObjectMapper` the configured JSON generator features are applied to
   * the mapper. These features can be amended programmatically by overriding this method and
   * return the features that are to be applied to the `ObjectMapper`.
   *
   * @param bindingName bindingName name of this `ObjectMapper`
   * @param configuredFeatures the list of `JsonGenerator.Feature` that were configured in `akka.serialization.jackson.json-generator-features`
   */
  def overrideConfiguredJsonGeneratorFeatures(
      @nowarn("msg=never used") bindingName: String,
      configuredFeatures: immutable.Seq[(JsonGenerator.Feature, Boolean)])
      : immutable.Seq[(JsonGenerator.Feature, Boolean)] =
    configuredFeatures

  /**
   * `StreamReadFeature`s used to configure the `JsonFactoryBuilder` that, if provided, will later be used to create
   * an `ObjectMapper`. These features can be amended programmatically by overriding this method and return the features
   * that are to be applied to the `JsonFactoryBuilder`.
   *
   * @param bindingName bindingName name of this `ObjectMapper`
   * @param configuredFeatures the list of `StreamReadFeature` that were configured in `akka.serialization.jackson.stream-read-features`
   */
  def overrideConfiguredStreamReadFeatures(
      @nowarn("msg=never used") bindingName: String,
      configuredFeatures: immutable.Seq[(StreamReadFeature, Boolean)]): immutable.Seq[(StreamReadFeature, Boolean)] =
    configuredFeatures

  /**
   * `StreamWriteFeature`s used to configure the `JsonFactoryBuilder` that, if provided, will later be used to create
   * an `ObjectMapper`. These features can be amended programmatically by overriding this method and return the features
   * that are to be applied to the `JsonFactoryBuilder`.
   *
   * @param bindingName bindingName name of this `ObjectMapper`
   * @param configuredFeatures the list of `StreamWriterFeature` that were configured in `akka.serialization.jackson.stream-write-features`
   */
  def overrideConfiguredStreamWriteFeatures(
      @nowarn("msg=never used") bindingName: String,
      configuredFeatures: immutable.Seq[(StreamWriteFeature, Boolean)]): immutable.Seq[(StreamWriteFeature, Boolean)] =
    configuredFeatures

  /**
   * `JsonReadFeature`s used to configure the `JsonFactoryBuilder` that, if provided, will later be used to create
   * an `ObjectMapper`. These features can be amended programmatically by overriding this method and return the features
   * that are to be applied to the `JsonFactoryBuilder`.
   *
   * @param bindingName bindingName name of this `ObjectMapper`
   * @param configuredFeatures the list of `JsonReadFeature` that were configured in `akka.serialization.jackson.json-read-features`
   */
  def overrideConfiguredJsonReadFeatures(
      @nowarn("msg=never used") bindingName: String,
      configuredFeatures: immutable.Seq[(JsonReadFeature, Boolean)]): immutable.Seq[(JsonReadFeature, Boolean)] =
    configuredFeatures

  /**
   * `JsonWriteFeature`s used to configure the `JsonFactoryBuilder` that, if provided, will later be used to create
   * an `ObjectMapper`. These features can be amended programmatically by overriding this method and return the features
   * that are to be applied to the `JsonFactoryBuilder`.
   *
   * @param bindingName bindingName name of this `ObjectMapper`
   * @param configuredFeatures the list of `JsonWriteFeature` that were configured in `akka.serialization.jackson.json-write-features`
   */
  def overrideConfiguredJsonWriteFeatures(
      @nowarn("msg=never used") bindingName: String,
      configuredFeatures: immutable.Seq[(JsonWriteFeature, Boolean)]): immutable.Seq[(JsonWriteFeature, Boolean)] =
    configuredFeatures

  /**
   * Visibility settings used to configure the `JsonFactoryBuilder` that, if provided, will later be used to create
   * an `ObjectMapper`. These settings can be amended programmatically by overriding this method and return the values
   * that are to be applied to the `JsonFactoryBuilder`.
   *
   * @param bindingName bindingName name of this `ObjectMapper`
   * @param configuredFeatures the list of `PropertyAccessor`/`JsonAutoDetect.Visibility` that were configured in
   *                           `akka.serialization.jackson.visibility`
   */
  def overrideConfiguredVisibility(
      @nowarn("msg=never used") bindingName: String,
      configuredFeatures: immutable.Seq[(PropertyAccessor, JsonAutoDetect.Visibility)])
      : immutable.Seq[(PropertyAccessor, JsonAutoDetect.Visibility)] =
    configuredFeatures

}
