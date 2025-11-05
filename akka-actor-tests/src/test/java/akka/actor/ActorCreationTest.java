/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor;

import static java.util.stream.Collectors.toCollection;
import static org.junit.Assert.*;

import akka.japi.Creator;
import java.util.ArrayList;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class ActorCreationTest extends JUnitSuite {

  static class C implements Creator<AbstractActor> {
    @Override
    public AbstractActor create() throws Exception {
      return null;
    }
  }

  static class D<T> implements Creator<T> {
    @Override
    public T create() {
      return null;
    }
  }

  static class E<T extends AbstractActor> implements Creator<T> {
    @Override
    public T create() {
      return null;
    }
  }

  static interface I<T> extends Creator<AbstractActor> {}

  static class F implements I<Object> {
    @Override
    public AbstractActor create() {
      return null;
    }
  }

  static class G implements Creator {
    public Object create() {
      return null;
    }
  }

  abstract class H extends AbstractActor {
    public H(String a) {}
  }

  static class P implements Creator<AbstractActor> {
    final String value;

    public P(String value) {
      this.value = value;
    }

    @Override
    public AbstractActor create() throws Exception {
      return null;
    }
  }

  public static class TestActor extends AbstractActor {
    public static Props propsUsingLamda(Integer magicNumber) {
      // You need to specify the actual type of the returned actor
      // since Java 8 lambdas have some runtime type information erased
      return Props.create(TestActor.class, () -> new TestActor(magicNumber));
    }

    @Deprecated
    public static Props propsUsingLamdaWithoutClass(Integer magicNumber) {
      return Props.create(() -> new TestActor(magicNumber));
    }

    @SuppressWarnings("unused")
    private final Integer magicNumber;

    TestActor(Integer magicNumber) {
      this.magicNumber = magicNumber;
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder().build();
    }
  }

  public static class TestActor2 extends UntypedAbstractActor {

    public static Props propsUsingCreator(final int magicNumber) {
      // You need to specify the actual type of the returned actor
      // since runtime type information erased
      return Props.create(
          TestActor2.class,
          new Creator<TestActor2>() {
            private static final long serialVersionUID = 1L;

            @Override
            public TestActor2 create() throws Exception {
              return new TestActor2(magicNumber);
            }
          });
    }

    public static Props propsUsingCreatorWithoutClass(final int magicNumber) {
      return Props.create(
          TestActor2.class,
          new Creator<TestActor2>() {
            private static final long serialVersionUID = 1L;

            @Override
            public TestActor2 create() throws Exception {
              return new TestActor2(magicNumber);
            }
          });
    }

    private static final Creator<TestActor2> staticCreator =
        new Creator<TestActor2>() {
          private static final long serialVersionUID = 1L;

          @Override
          public TestActor2 create() throws Exception {
            return new TestActor2(12);
          }
        };

    public static Props propsUsingStaticCreator(final int magicNumber) {

      return Props.create(TestActor2.class, staticCreator);
    }

    final int magicNumber;

    TestActor2(int magicNumber) {
      this.magicNumber = magicNumber;
    }

    @Override
    public void onReceive(Object msg) {}
  }

  public static class Issue20537Reproducer extends UntypedAbstractActor {

    static final class ReproducerCreator implements Creator<Issue20537Reproducer> {

      final boolean create;

      private ReproducerCreator(boolean create) {
        this.create = create;
      }

      @Override
      public Issue20537Reproducer create() throws Exception {
        return new Issue20537Reproducer(create);
      }
    }

    public Issue20537Reproducer(boolean create) {}

    @Override
    public void onReceive(Object message) throws Exception {}
  }

  @Test
  public void testWrongAnonymousInPlaceCreator() {
    IllegalArgumentException exception =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
                Props.create(
                    Actor.class,
                    new Creator<Actor>() {
                      @Override
                      public Actor create() throws Exception {
                        return null;
                      }
                    }));
    assertEquals(
        "cannot use non-static local Creator to create actors; make it static (e.g. local to a static method) or top-level",
        exception.getMessage());
  }

  @Test
  @SuppressWarnings("unchecked")
  @Deprecated
  public void testWrongErasedStaticCreator() {
    IllegalArgumentException exception =
        Assert.assertThrows(IllegalArgumentException.class, () -> Props.create(new G()));
    assertEquals(
        "erased Creator types (e.g. lambdas) are unsupported, use Props.create(actorClass, creator) instead",
        exception.getMessage());
    Props.create(AbstractActor.class, new G());
  }

  @Deprecated
  @Test
  public void testRightStaticCreator() {
    final Props p = Props.create(new C());
    assertEquals(AbstractActor.class, p.actorClass());
  }

  @Test
  @Deprecated
  public void testWrongAnonymousClassStaticCreator() {
    IllegalArgumentException exception =
        Assert.assertThrows(
            "Should have detected this is not a real static class, and thrown",
            IllegalArgumentException.class,
            () -> {
              Props.create(new C() {}); // has implicit reference to outer class
            });
    assertEquals(
        "cannot use non-static local Creator to create actors; make it static (e.g. local to a static method) or top-level",
        exception.getMessage());
  }

  @Test
  public void testRightTopLevelNonStaticCreator() {
    final Creator<UntypedAbstractActor> nonStatic = new NonStaticCreator();
    final Props p = Props.create(UntypedAbstractActor.class, nonStatic);
    assertEquals(UntypedAbstractActor.class, p.actorClass());
  }

  @Test
  @Deprecated
  public void testRightStaticParametricCreator() {
    final Props p = Props.create(new D<AbstractActor>());
    assertEquals(Actor.class, p.actorClass());
  }

  @Test
  @Deprecated
  public void testRightStaticBoundedCreator() {
    final Props p = Props.create(new E<AbstractActor>());
    assertEquals(AbstractActor.class, p.actorClass());
  }

  @Test
  @Deprecated
  public void testRightStaticSuperinterface() {
    final Props p = Props.create(new F());
    assertEquals(AbstractActor.class, p.actorClass());
  }

  @Test
  public void testWrongAbstractActorClass() {
    IllegalArgumentException exception =
        Assert.assertThrows(IllegalArgumentException.class, () -> Props.create(H.class, "a"));
    assertEquals(
        String.format("Actor class [%s] must not be abstract", H.class.getName()),
        exception.getMessage());
  }

  private static Creator<AbstractActor> createAnonymousCreatorInStaticMethod() {
    return new Creator<AbstractActor>() {
      @Override
      public AbstractActor create() throws Exception {
        return null;
      }
    };
  }

  @Test
  @Deprecated
  public void testAnonymousClassCreatedInStaticMethodCreator() {
    final Creator<AbstractActor> anonymousCreatorFromStaticMethod =
        createAnonymousCreatorInStaticMethod();
    Props.create(anonymousCreatorFromStaticMethod);
  }

  @Test
  @Deprecated
  public void testClassCreatorWithArguments() {
    final Creator<AbstractActor> anonymousCreatorFromStaticMethod = new P("hello");
    Props.create(anonymousCreatorFromStaticMethod);
  }

  @Test
  @Deprecated
  public void testAnonymousClassCreatorWithArguments() {
    IllegalArgumentException exception =
        Assert.assertThrows(
            "Should have detected this is not a real static class, and thrown",
            IllegalArgumentException.class,
            () -> {
              final Creator<AbstractActor> anonymousCreatorFromStaticMethod = new P("hello") {
                    // captures enclosing class
                  };
              Props.create(anonymousCreatorFromStaticMethod);
            });
    assertEquals(
        "cannot use non-static local Creator to create actors; make it static (e.g. local to a static method) or top-level",
        exception.getMessage());
  }

  @Test
  public void testRightPropsUsingLambda() {
    final Props p = TestActor.propsUsingLamda(17);
    assertEquals(TestActor.class, p.actorClass());
  }

  @Test
  @Deprecated
  public void testWrongPropsUsingLambdaWithoutClass() {
    final Props p = TestActor.propsUsingLamda(17);
    assertEquals(TestActor.class, p.actorClass());

    IllegalArgumentException exception =
        Assert.assertThrows(
            "Should have detected lambda erasure, and thrown",
            IllegalArgumentException.class,
            () -> TestActor.propsUsingLamdaWithoutClass(17));
    assertEquals(
        "erased Creator types (e.g. lambdas) are unsupported, use Props.create(actorClass, creator) instead",
        exception.getMessage());
  }

  @Test
  public void testRightPropsUsingCreator() {
    final Props p = TestActor2.propsUsingCreator(17);
    assertEquals(TestActor2.class, p.actorClass());
  }

  @Test
  public void testPropsUsingCreatorWithoutClass() {
    final Props p = TestActor2.propsUsingCreatorWithoutClass(17);
    assertEquals(TestActor2.class, p.actorClass());
  }

  @Test
  public void testIssue20537Reproducer() {
    final Issue20537Reproducer.ReproducerCreator creator =
        new Issue20537Reproducer.ReproducerCreator(false);
    final Props p = Props.create(Issue20537Reproducer.class, creator);
    assertEquals(Issue20537Reproducer.class, p.actorClass());

    ArrayList<Props> pList =
        IntStream.range(0, 4)
            .mapToObj(i -> Props.create(Issue20537Reproducer.class, creator))
            .collect(toCollection(ArrayList::new));
    for (Props each : pList) {
      assertEquals(Issue20537Reproducer.class, each.actorClass());
    }
  }
}
