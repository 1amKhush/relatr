import { RelayPool } from "applesauce-relay";
import type { NostrEvent } from "nostr-tools";
import type {
  NostrProfile,
  RelatrConfig,
  SearchProfilesParams,
  SearchProfilesResult,
} from "../types";
import type { MetadataRepository } from "../database/repositories/MetadataRepository";
import type { SocialGraph } from "../graph/SocialGraph";
import type { MetricsValidator } from "../validators/MetricsValidator";
import type { TrustCalculator } from "../trust/TrustCalculator";
import type { ISearchService } from "./ServiceInterfaces";
import { ValidationError } from "../types";
import { logger } from "../utils/Logger";
import { SEARCH_RELAYS } from "@/constants/nostr";
import { nowMs } from "@/utils/utils";

type SearchCandidate = {
  pubkey: string;
  relevanceMultiplier: number;
  isExactMatch: boolean;
};

type RankedSearchCandidate = {
  pubkey: string;
  trustScore: number;
  exactMatch: boolean;
  rawTrustScore: number;
  rankingScore: number;
  relevanceMultiplier: number;
};

export class SearchService implements ISearchService {
  private static readonly MAX_TEXT_RELEVANCE_SCORE = 1.4;
  private static readonly TRUST_RANKING_WEIGHT = 0.75;
  private static readonly TEXT_RANKING_WEIGHT = 0.25;
  private static readonly EXACT_MATCH_RANKING_BOOST = 0.1;

  constructor(
    private config: RelatrConfig,
    private metadataRepository: MetadataRepository,
    private socialGraph: SocialGraph,
    private metricsValidator: MetricsValidator,
    private trustCalculator: TrustCalculator,
    private pool: RelayPool,
  ) {}

  async searchProfiles(
    params: SearchProfilesParams,
  ): Promise<SearchProfilesResult> {
    const { query, limit = 5, sourcePubkey, extendToNostr } = params;
    const normalizedQuery = this.normalizeSearchQuery(query);
    const normalizedLimit = this.normalizeSearchLimit(limit);

    if (!normalizedQuery) {
      throw new ValidationError("Invalid search query", "query");
    }

    const effectiveSourcePubkey =
      sourcePubkey ||
      this.socialGraph.getCurrentRoot() ||
      this.config.defaultSourcePubkey;
    const startTime = nowMs();

    // Search local database - DuckDB scans all fresh metadata matches and returns
    // a broad candidate window ranked primarily by text relevance.
    const localResults = await this.metadataRepository.search(
      normalizedQuery,
      normalizedLimit,
      this.config.decayFactor,
    );

    logger.debug(
      `🔍 Found ${localResults.length} text-ranked profiles from database`,
    );

    // Prepare profiles for trust scoring
    const profilesForScoring = localResults.map((r) => ({
      pubkey: r.pubkey,
      relevanceMultiplier: r.score,
      isExactMatch: r.isExactMatch,
    }));

    // Extend to Nostr if needed
    const nostrProfiles = await this.extendSearchWithNostr(
      normalizedQuery,
      normalizedLimit,
      profilesForScoring,
      extendToNostr,
    );

    // Process and return final results
    return this.processSearchResults(
      [...profilesForScoring, ...nostrProfiles],
      effectiveSourcePubkey,
      normalizedLimit,
      startTime,
    );
  }

  private normalizeSearchQuery(query: unknown): string {
    if (typeof query !== "string") {
      return "";
    }

    return query.trim().replace(/\s+/g, " ");
  }

  private normalizeSearchLimit(limit: number): number {
    if (!Number.isFinite(limit)) {
      return 5;
    }

    return Math.max(1, Math.floor(limit));
  }

  /**
   * Extend search to Nostr relays for additional results
   */
  private async extendSearchWithNostr(
    query: string,
    limit: number,
    profilesForScoring: SearchCandidate[],
    extendToNostr?: boolean,
  ): Promise<SearchCandidate[]> {
    const nostrProfiles: SearchCandidate[] = [];
    const seenPubkeys = new Set(
      profilesForScoring.map((profile) => profile.pubkey),
    );
    const shouldExtendToNostr =
      extendToNostr || profilesForScoring.length === 0;

    if (!shouldExtendToNostr || !this.pool) {
      return nostrProfiles;
    }

    const remaining = Math.max(0, limit - profilesForScoring.length);
    if (remaining <= 0) {
      return nostrProfiles;
    }

    logger.debug(
      `🔍 Extending search to Nostr relays for up to ${remaining} results`,
    );

    const searchFilter = { kinds: [0], search: query, limit: remaining };

    try {
      const nostrEvents = await new Promise<NostrEvent[]>((resolve, reject) => {
        const events: NostrEvent[] = [];
        const subscription = this.pool
          .request(SEARCH_RELAYS, searchFilter)
          .subscribe({
            next: (event) => events.push(event),
            error: (error) => reject(error),
            complete: () => resolve(events),
          });

        setTimeout(() => {
          subscription.unsubscribe();
          resolve(events);
        }, this.config.capTimeoutMs);
      });

      for (const event of nostrEvents) {
        if (!seenPubkeys.has(event.pubkey)) {
          const parsedProfile: unknown = JSON.parse(event.content);
          if (!parsedProfile || typeof parsedProfile !== "object") {
            continue;
          }

          const profile = parsedProfile as NostrProfile;
          const { multiplier, isExactMatch } =
            this.calculateRelevanceMultiplier(profile, query);
          nostrProfiles.push({
            pubkey: event.pubkey,
            relevanceMultiplier: multiplier,
            isExactMatch,
          });
          seenPubkeys.add(event.pubkey);
          this.metadataRepository
            .save({ ...profile, pubkey: event.pubkey })
            .catch((err) => {
              logger.warn(`Failed to cache profile for ${event.pubkey}:`, err);
            });
        }
      }
    } catch {
      // Remote search failed, continue with local results only
      logger.debug(
        "Nostr relay search failed, continuing with local results only",
      );
    }

    return nostrProfiles;
  }

  /**
   * Process search results by calculating trust scores and formatting output
   */
  private async processSearchResults(
    finalProfiles: SearchCandidate[],
    effectiveSourcePubkey: string,
    limit: number,
    startTime: number,
  ): Promise<SearchProfilesResult> {
    const dedupedProfiles = this.dedupeSearchCandidates(finalProfiles);
    const profilesWithScores = await this.calculateProfileScores(
      dedupedProfiles,
      effectiveSourcePubkey,
    );

    // Keep trust as the primary ranking signal while using deterministic
    // tie-breakers to stabilize equal displayed scores.
    profilesWithScores.sort((a, b) => {
      return (
        b.rankingScore - a.rankingScore ||
        b.rawTrustScore - a.rawTrustScore ||
        Number(b.exactMatch) - Number(a.exactMatch) ||
        b.relevanceMultiplier - a.relevanceMultiplier ||
        a.pubkey.localeCompare(b.pubkey)
      );
    });

    const results = profilesWithScores.slice(0, limit).map((item, index) => ({
      pubkey: item.pubkey,
      trustScore: item.trustScore,
      rank: index + 1,
      exactMatch: item.exactMatch,
    }));

    const endTime = nowMs();
    logger.info(`Search completed in ${endTime - startTime}ms`);

    return {
      results,
      totalFound: results.length,
      searchTimeMs: endTime - startTime,
    };
  }

  private dedupeSearchCandidates(
    candidates: SearchCandidate[],
  ): SearchCandidate[] {
    const byPubkey = new Map<string, SearchCandidate>();

    for (const candidate of candidates) {
      const existing = byPubkey.get(candidate.pubkey);
      if (
        !existing ||
        Number(candidate.isExactMatch) > Number(existing.isExactMatch) ||
        candidate.relevanceMultiplier > existing.relevanceMultiplier
      ) {
        byPubkey.set(candidate.pubkey, candidate);
      }
    }

    return [...byPubkey.values()];
  }

  async calculateProfileScores(
    profiles: SearchCandidate[],
    effectiveSourcePubkey: string,
  ): Promise<RankedSearchCandidate[]> {
    if (profiles.length === 0) {
      return [];
    }

    try {
      // Extract pubkeys for batch operations
      const profilePubkeys = profiles.map((p) => p.pubkey);

      // Pre-fetch all distances and metrics in parallel
      const [distances, metricsMap] = await Promise.all([
        this.socialGraph.getDistancesBatch(profilePubkeys),
        this.metricsValidator.getStoredMetrics(
          profilePubkeys,
          effectiveSourcePubkey,
        ),
      ]);

      // Calculate trust scores with pre-fetched data
      const results = profiles.map(
        ({ pubkey, relevanceMultiplier, isExactMatch }) => {
          try {
            const distance = distances.get(pubkey) || 1000;
            const metrics = metricsMap.get(pubkey);

            if (!metrics) {
              return {
                pubkey,
                trustScore: 0,
                rawTrustScore: 0,
                rankingScore: 0,
                exactMatch: isExactMatch,
                relevanceMultiplier,
              };
            }

            const trustScore = this.trustCalculator.calculate(
              effectiveSourcePubkey,
              pubkey,
              metrics,
              distance,
            );

            const roundedTrustScore = Number(
              Math.max(0, Math.min(1, trustScore.score)).toFixed(2),
            );
            const normalizedTextScore = Math.max(
              0,
              Math.min(
                1,
                relevanceMultiplier / SearchService.MAX_TEXT_RELEVANCE_SCORE,
              ),
            );
            const rankingScore =
              SearchService.TRUST_RANKING_WEIGHT * trustScore.score +
              SearchService.TEXT_RANKING_WEIGHT * normalizedTextScore +
              (isExactMatch ? SearchService.EXACT_MATCH_RANKING_BOOST : 0);

            return {
              pubkey,
              trustScore: roundedTrustScore,
              rawTrustScore: trustScore.score,
              rankingScore,
              exactMatch: isExactMatch,
              relevanceMultiplier,
            };
          } catch (error) {
            logger.warn(
              `Failed to calculate trust score for ${pubkey}:`,
              error instanceof Error ? error.message : String(error),
            );
            return {
              pubkey,
              trustScore: 0,
              rawTrustScore: 0,
              rankingScore: 0,
              exactMatch: isExactMatch,
              relevanceMultiplier,
            };
          }
        },
      );

      return results.map((result) => ({
        pubkey: result.pubkey,
        trustScore: result.trustScore,
        exactMatch: result.exactMatch,
        rawTrustScore: result.rawTrustScore,
        rankingScore: result.rankingScore,
        relevanceMultiplier: result.relevanceMultiplier,
      }));
    } catch (error) {
      logger.warn(
        "Batch scoring failed, returning zeroed search scores:",
        error instanceof Error ? error.message : String(error),
      );

      return profiles.map(({ pubkey, isExactMatch }) => ({
        pubkey,
        trustScore: 0,
        exactMatch: isExactMatch,
        rawTrustScore: 0,
        rankingScore: 0,
        relevanceMultiplier: 1,
      }));
    }
  }

  calculateRelevanceMultiplier(
    profile: NostrProfile,
    query: string,
  ): { multiplier: number; isExactMatch: boolean } {
    const queryLower = this.normalizeSearchQuery(query).toLowerCase();
    let isExactMatch = false;

    for (const field of ["name", "display_name", "nip05", "lud16"] as const) {
      const fieldValue = profile[field as keyof NostrProfile];
      if (typeof fieldValue === "string" && fieldValue.trim()) {
        const valueLower = fieldValue.toLowerCase();

        if (valueLower === queryLower) {
          isExactMatch = true;
          break;
        }
      }
    }

    return {
      multiplier: 1,
      isExactMatch,
    };
  }
}
