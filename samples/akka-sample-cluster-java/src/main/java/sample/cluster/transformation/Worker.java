package sample.cluster.transformation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import sample.cluster.CborSerializable;

//#worker
public class Worker {

  public static ServiceKey<Worker.TransformText> WORKER_SERVICE_KEY =
      ServiceKey.create(TransformText.class, "Worker");

  interface Command extends CborSerializable {}

  public static final class TransformText implements Command {
    public final String text;
    public TransformText(String text, ActorRef<TextTransformed> replyTo) {
      this.text = text;
      this.replyTo = replyTo;
    }
    public final ActorRef<TextTransformed> replyTo;
  }
  public static final class TextTransformed implements CborSerializable {
    public final String text;
    @JsonCreator
    public TextTransformed(String text) {
      this.text = text;
    }
  }

  public static Behavior<Command> create() {
    return Behaviors.setup(context -> {
      context.getLog().info("Registering myself with receptionist");
      context.getSystem().receptionist().tell(Receptionist.register(WORKER_SERVICE_KEY, context.getSelf().narrow()));

      return Behaviors.receive(Command.class)
          .onMessage(TransformText.class, command -> {
            command.replyTo.tell(new TextTransformed(command.text.toUpperCase()));
            return Behaviors.same();
          }).build();
    });
  }
}
//#worker
