import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameState } from "../internal/gamelogic/gamestate.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";
import type { ConfirmChannel } from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { handleWar, WarOutcome, type WarResolution } from "../internal/gamelogic/war.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return function (ps: PlayingState): AckType {
    try {
      return handlePause(gs, ps);
    } finally {
      process.stdout.write("> ");
    }
  };
}

export function handlerMove(gs: GameState, channel: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
  return async function (move: ArmyMove): Promise<AckType> {
    try {
      const moveOutcome = handleMove(gs, move);
      switch (moveOutcome) {
        case MoveOutcome.SamePlayer:
          return AckType.NackDiscard;
        case MoveOutcome.Safe:
          return AckType.Ack;
        case MoveOutcome.MakeWar: {
          const rw: RecognitionOfWar = {
            attacker: move.player,
            defender: gs.getPlayerSnap(),
          };
          await publishJSON(channel, ExchangePerilTopic, `${WarRecognitionsPrefix}.${rw.defender.username}`, rw);
          return AckType.Ack;
        }
      }
    } catch (err) {
      return AckType.NackRequeue
    } finally {
      process.stdout.write("> ");
    }
  };
}

export function handlerWar(gs: GameState): (rw: RecognitionOfWar) => AckType {
  return function (rw: RecognitionOfWar): AckType {
    try {
      const resolution = handleWar(gs, rw);
      switch (resolution.result) {
        case WarOutcome.NotInvolved: 
          return AckType.NackRequeue;
        case WarOutcome.NoUnits:
          return AckType.NackDiscard;
        case WarOutcome.OpponentWon:
          return AckType.Ack
        case WarOutcome.YouWon:
          return AckType.Ack;
        case WarOutcome.Draw:
          return AckType.Ack;
        default:
          console.error("Unknown war outcome.");
          return AckType.NackDiscard;
        }
    } finally {
      process.stdout.write("> ");
    }
  };
}
