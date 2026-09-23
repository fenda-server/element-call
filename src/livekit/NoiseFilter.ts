/*
Copyright 2026 Varanda contributors.

SPDX-License-Identifier: AGPL-3.0-only
Please see LICENSE in the repository root for full details.
*/

import {
  LocalAudioTrack,
  type Room as LivekitRoom,
  Track,
  TrackEvent,
} from "livekit-client";
import {
  combineLatest,
  fromEvent,
  map,
  merge,
  of,
  startWith,
  switchMap,
} from "rxjs";
import { type Logger } from "matrix-js-sdk/lib/logger";

import type * as KrispModule from "@livekit/krisp-noise-filter";
import { noiseFilter as noiseFilterSetting } from "../settings/settings";
import { observeTrackReference$ } from "../state/observeTrackReference";
import { type ObservableScope } from "../state/ObservableScope";

// Must match KrispNoiseFilterProcessor.name
const KRISP_PROCESSOR_NAME = "livekit-noise-filter";

type Krisp = typeof KrispModule;

let krisp: Promise<Krisp> | undefined;

// The Krisp package bundles its model (several MB), so only load it on demand
const loadKrisp = async (): Promise<Krisp> =>
  (krisp ??= import("@livekit/krisp-noise-filter"));

/**
 * Whether the Krisp noise filter can run in this browser
 * (e.g. Safari < 17.4 is not supported).
 */
export async function supportsNoiseFilter(): Promise<boolean> {
  try {
    return (await loadKrisp()).isKrispNoiseFilterSupported();
  } catch {
    return false;
  }
}

/**
 * Keeps the Krisp noise filter applied to the local microphone track
 * according to the noise filter setting. Krisp only works with LiveKit Cloud.
 *
 * The filter is only attached while the track's upstream is not paused:
 * attaching a processor replaces the sender's track, which would otherwise
 * start sending audio before we intend to publish.
 */
export const noiseFilterSync = (
  scope: ObservableScope,
  room: LivekitRoom,
  logger: Logger,
): void => {
  const micTrack$ = observeTrackReference$(
    room.localParticipant,
    Track.Source.Microphone,
  ).pipe(
    map((trackRef) => {
      const track = trackRef?.publication.track;
      return track instanceof LocalAudioTrack ? track : null;
    }),
    // Re-evaluate whenever the upstream gets paused or resumed
    switchMap((track) =>
      track
        ? merge(
            fromEvent(track, TrackEvent.UpstreamPaused),
            fromEvent(track, TrackEvent.UpstreamResumed),
          ).pipe(
            startWith(null),
            map(() => track),
          )
        : of(null),
    ),
  );

  // Serialize processor changes so that we never attach two filters at once
  let pending = Promise.resolve();

  combineLatest([micTrack$, noiseFilterSetting.value$])
    .pipe(scope.bind())
    .subscribe(([track, enabled]) => {
      pending = pending
        .then(async () => {
          if (!track) return;
          const active = track.getProcessor()?.name === KRISP_PROCESSOR_NAME;
          if (enabled && !active && !track.isUpstreamPaused) {
            const {
              isKrispNoiseFilterSupported,
              KrispNoiseFilter: createKrisp,
            } = await loadKrisp();
            if (!isKrispNoiseFilterSupported()) {
              logger.info(
                "Krisp noise filter is not supported by this browser",
              );
              return;
            }
            // The track may have been paused while loading Krisp
            if (track.isUpstreamPaused) return;
            logger.info("Enabling Krisp noise filter");
            const processor = createKrisp();
            await track.setProcessor(processor);
            await processor.setEnabled(true);
          } else if (!enabled && active) {
            logger.info("Disabling Krisp noise filter");
            await track.stopProcessor();
          }
        })
        .catch((e) => {
          logger.error("Failed to update Krisp noise filter", e);
        });
    });
};
