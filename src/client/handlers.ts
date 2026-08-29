import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState } from "../internal/gamelogic/gamestate.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => void {
    return function (ps: PlayingState): void {
        handlePause(gs, ps);
        process.stdout.write("> ");
    };
}

export function handlerMove(gs: GameState): (move: ArmyMove) => void {
    return function (move: ArmyMove): void {
        handleMove(gs, move);
        process.stdout.write("> ");
    };
}