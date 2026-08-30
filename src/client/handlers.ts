import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState } from "../internal/gamelogic/gamestate.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import type { AckType } from "../internal/pubsub/consume.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return function (ps: PlayingState): AckType {
    try {
      return handlePause(gs, ps);
    } finally {
      process.stdout.write("> ");
    }
  };
}

export function handlerMove(gs: GameState): (move: ArmyMove) => AckType {
  return function (move: ArmyMove): AckType {
    try {
      return handleMove(gs, move);
    } finally {
      process.stdout.write("> ");
    }
  };
}