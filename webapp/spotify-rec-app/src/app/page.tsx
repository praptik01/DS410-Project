"use client";

import { useEffect, useMemo, useState } from "react";

type SongOption = {
  title: string;
  artist: string;
};

const MIN_QUERY_LENGTH = 2;

export default function Home() {
  const [searchTerm, setSearchTerm] = useState("");
  const [suggestions, setSuggestions] = useState<SongOption[]>([]);
  const [loadingSuggestions, setLoadingSuggestions] = useState(false);
  const [suggestionsError, setSuggestionsError] = useState<string | null>(null);

  const [playlist, setPlaylist] = useState<SongOption[]>([]);
  const [predictions, setPredictions] = useState<string[]>([]);
  const [predicting, setPredicting] = useState(false);
  const [predictionError, setPredictionError] = useState<string | null>(null);

  useEffect(() => {
    const trimmedQuery = searchTerm.trim();
    if (trimmedQuery.length < MIN_QUERY_LENGTH) {
      setSuggestions([]);
      setLoadingSuggestions(false);
      setSuggestionsError(null);
      return;
    }

    const controller = new AbortController();
    setLoadingSuggestions(true);
    setSuggestionsError(null);

    const timeoutId = window.setTimeout(async () => {
      try {
        const response = await fetch(
          `/api/songs?q=${encodeURIComponent(trimmedQuery)}&limit=40`,
          { signal: controller.signal },
        );
        if (!response.ok) {
          throw new Error("Unable to fetch songs");
        }
        const data = await response.json();
        const options = Array.isArray(data?.songs)
          ? (data.songs as SongOption[])
          : [];
        setSuggestions(options);
      } catch (error) {
        if ((error as Error).name === "AbortError") {
          return;
        }
        console.error(error);
        setSuggestionsError("Unable to load songs right now. Please try again.");
        setSuggestions([]);
      } finally {
        setLoadingSuggestions(false);
      }
    }, 200);

    return () => {
      controller.abort();
      window.clearTimeout(timeoutId);
    };
  }, [searchTerm]);

  const filteredSuggestions = useMemo(() => {
    if (!suggestions.length) {
      return [];
    }
    const taken = new Set(
      playlist.map((entry) => `${entry.title.toLowerCase()}__${entry.artist.toLowerCase()}`),
    );
    return suggestions.filter((option: SongOption) => {
      const key = `${option.title?.toLowerCase() ?? ""}__${option.artist?.toLowerCase() ?? ""}`;
      return !taken.has(key);
    });
  }, [suggestions, playlist]);

  const handleAddSong = (option: SongOption) => {
    setPlaylist((current) => {
      const exists = current.some(
        (entry) =>
          entry.title.toLowerCase() === option.title.toLowerCase() &&
          entry.artist.toLowerCase() === option.artist.toLowerCase(),
      );
      if (exists) {
        return current;
      }
      return [...current, option];
    });
    setSearchTerm("");
    setSuggestions([]);
    setPredictions([]);
    setPredictionError(null);
  };

  const handleRemoveSong = (option: SongOption) => {
    setPlaylist((current) =>
      current.filter(
        (entry) =>
          entry.title.toLowerCase() !== option.title.toLowerCase() ||
          entry.artist.toLowerCase() !== option.artist.toLowerCase(),
      ),
    );
    setPredictions([]);
    setPredictionError(null);
  };

  const handlePredict = () => {
    if (!playlist.length) {
      setPredictionError("Add at least one track before requesting predictions.");
      return;
    }

    setPredictionError(null);
    setPredicting(true);

    setTimeout(() => {
      const recommended = playlist.slice(0, 10).map((entry, index) => {
        const rank = index + 1;
        return `Track ${rank}: curated to complement "${entry.title}" by ${entry.artist || "Unknown"}`;
      });
      setPredictions(recommended);
      setPredicting(false);
    }, 300);
  };

  return (
    <div className="min-h-screen bg-[#121212] text-white">
      <div className="relative isolate overflow-hidden bg-gradient-to-br from-[#1db954]/20 via-[#191414] to-[#000000]">
        <div className="mx-auto max-w-6xl px-6 pb-20 pt-16 lg:px-10">
          <header className="mb-14 flex flex-col gap-6 sm:flex-row sm:items-end sm:justify-between">
            <div className="space-y-4">
              <p className="text-xs uppercase tracking-[0.5em] text-[#1db954]">
                Spotify vibe lab
              </p>
              <div className="space-y-4">
                <h1 className="text-4xl font-semibold leading-tight sm:text-5xl">
                  Spotify Playlist Predictor
                </h1>
                <p className="max-w-2xl text-sm text-white/70">
                 Select your favorite tracks and let our recommendation engine suggest the perfect playlist
                 to match your vibe.
                </p>
              </div>
            </div>
            <div className="flex shrink-0 flex-col items-start gap-2 rounded-2xl border border-white/5 bg-white/5 px-5 py-4 text-xs uppercase tracking-[0.25em] text-white/70 backdrop-blur">
              
            </div>
          </header>

          <main className="grid gap-8 lg:grid-cols-[1.2fr_0.8fr]">
            <section className="flex flex-col gap-6 rounded-[28px] bg-white/5 p-8 shadow-[0_32px_80px_-40px_rgba(0,0,0,0.8)] ring-1 ring-white/10 backdrop-blur">
              <div className="space-y-3">
                <h2 className="text-xl font-semibold">Find a track</h2>
                <p className="text-sm text-white/70">
                  Start typing to search the full dataset. Matches include both title and primary
                  artist — just like Spotify&apos;s quick search.
                </p>
              </div>

              <div className="space-y-4">
                <div className="relative">
                  <input
                    value={searchTerm}
                    onChange={(event) => setSearchTerm(event.target.value)}
                    placeholder="Search songs or artists"
                    className="w-full rounded-full border border-white/10 bg-white/[0.06] px-6 py-3 text-sm text-white outline-none transition focus:border-[#1db954]/70 focus:ring-4 focus:ring-[#1db954]/20"
                  />
                  <span className="pointer-events-none absolute right-6 top-1/2 hidden -translate-y-1/2 text-xs uppercase tracking-[0.35em] text-white/50 sm:inline">
                    Search
                  </span>
                </div>

                {searchTerm.trim().length > 0 && searchTerm.trim().length < MIN_QUERY_LENGTH ? (
                  <p className="rounded-2xl border border-white/5 bg-white/[0.04] px-4 py-3 text-xs text-white/60">
                    Keep typing to see matching songs (minimum {MIN_QUERY_LENGTH} characters).
                  </p>
                ) : null}

                {suggestionsError ? (
                  <p className="rounded-2xl border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-200">
                    {suggestionsError}
                  </p>
                ) : null}

                <div className="space-y-2">
                  {loadingSuggestions ? (
                    <div className="flex items-center justify-center gap-3 rounded-2xl border border-white/5 bg-white/[0.05] px-4 py-5 text-sm text-white/60">
                      <span className="inline-flex h-2 w-2 animate-pulse rounded-full bg-[#1db954]" />
                      Searching the catalog…
                    </div>
                  ) : filteredSuggestions.length ? (
                    <ul className="max-h-64 space-y-2 overflow-y-auto rounded-2xl border border-white/5 bg-black/40 p-3 shadow-inner">
                      {filteredSuggestions.map((option, index) => (
                        <li
                          key={`${option.title}-${option.artist}-${index}`}
                          className="group flex items-center justify-between gap-4 rounded-[20px] border border-transparent bg-white/[0.02] px-4 py-3 text-sm transition hover:border-[#1db954]/40 hover:bg-white/[0.08]"
                        >
                          <div className="flex flex-col">
                            <span className="font-semibold text-white group-hover:text-[#1db954]">
                              {option.title}
                            </span>
                            {option.artist ? (
                              <span className="text-xs uppercase tracking-[0.25em] text-white/50">
                                {option.artist}
                              </span>
                            ) : null}
                          </div>
                          <button
                            type="button"
                            onClick={() => handleAddSong(option)}
                            className="rounded-full border border-[#1db954]/60 px-4 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-[#1db954] transition hover:border-[#1db954] hover:bg-[#1db954] hover:text-black"
                          >
                            Add
                          </button>
                        </li>
                      ))}
                    </ul>
                  ) : searchTerm.trim().length >= MIN_QUERY_LENGTH ? (
                    <div className="rounded-2xl border border-white/5 bg-white/[0.05] px-4 py-5 text-sm text-white/60">
                      No songs found yet — try a different title or include the artist name.
                    </div>
                  ) : null}
                </div>

                <div className="flex items-center gap-3">
                  <button
                    type="button"
                    onClick={() => {
                      setPlaylist([]);
                      setPredictions([]);
                      setPredictionError(null);
                    }}
                    className="inline-flex items-center justify-center rounded-full border border-white/10 px-5 py-2 text-sm font-medium text-white/80 transition hover:border-white/40 hover:text-white disabled:cursor-not-allowed disabled:opacity-50"
                    disabled={!playlist.length}
                  >
                    Clear playlist
                  </button>
                  <span className="text-xs uppercase tracking-[0.35em] text-white/40">
                    {playlist.length} track{playlist.length === 1 ? "" : "s"} added
                  </span>
                </div>
              </div>

              <div className="space-y-4 rounded-[24px] border border-white/5 bg-black/40 p-5 shadow-inner">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-semibold uppercase tracking-[0.35em] text-white/60">
                    Seed playlist
                  </h3>
                  {playlist.length ? (
                    <span className="text-xs uppercase tracking-[0.35em] text-[#1db954]">
                      {playlist.length} ready
                    </span>
                  ) : null}
                </div>
                {playlist.length ? (
                  <ol className="space-y-2 text-sm">
                    {playlist.map((entry, index) => (
                      <li
                        key={`${entry.title}-${entry.artist}-${index}`}
                        className="flex items-center justify-between gap-4 rounded-[18px] border border-white/5 bg-white/[0.04] px-4 py-3 transition hover:border-[#1db954]/30 hover:bg-white/[0.07]"
                      >
                        <div className="flex items-center gap-4">
                          <span className="inline-flex h-8 w-8 items-center justify-center rounded-full bg-black/50 text-xs font-semibold text-white/60">
                            {index + 1}
                          </span>
                          <div>
                            <p className="font-semibold text-white">{entry.title}</p>
                            {entry.artist ? (
                              <p className="text-xs uppercase tracking-[0.2em] text-white/50">
                                {entry.artist}
                              </p>
                            ) : null}
                          </div>
                        </div>
                        <button
                          type="button"
                          onClick={() => handleRemoveSong(entry)}
                          className="text-xs uppercase tracking-[0.3em] text-white/50 transition hover:text-[#1db954]"
                        >
                          Remove
                        </button>
                      </li>
                    ))}
                  </ol>
                ) : (
                  <div className="flex h-32 flex-col items-center justify-center gap-2 rounded-[20px] border border-dashed border-white/10 bg-white/[0.03] text-center text-sm text-white/50">
                    <span className="text-2xl">🎧</span>
                    <p>Search for a track to start building your playlist.</p>
                  </div>
                )}
              </div>
            </section>

            <section className="flex flex-col gap-6 rounded-[28px] bg-gradient-to-br from-[#1db954]/15 via-black/40 to-black/80 p-8 shadow-[0_32px_80px_-40px_rgba(0,0,0,0.9)] ring-1 ring-[#1db954]/30">
              <div className="space-y-3">
                <h2 className="text-xl font-semibold text-white">
                  Prediction queue
                </h2>
                <p className="text-sm text-white/70">
                 This panel will populate with recommended tracks based on your playlist.
                </p>
              </div>

              <div className="flex items-center gap-3">
                <button
                  type="button"
                  onClick={handlePredict}
                  disabled={predicting}
                  className="inline-flex items-center justify-center rounded-full bg-[#1db954] px-6 py-2 text-sm font-semibold text-black transition hover:bg-[#1ed760] disabled:cursor-not-allowed disabled:bg-[#1db954]/40 disabled:text-black/60"
                >
                  {predicting ? "Generating…" : "Predict playlist"}
                </button>
                
              </div>

              {predictionError ? (
                <p className="rounded-2xl border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                  {predictionError}
                </p>
              ) : null}

              <div className="rounded-[24px] border border-[#1db954]/20 bg-black/40 p-5">
                {predictions.length ? (
                  <ol className="space-y-3 text-sm text-white/90">
                    {predictions.map((item, index) => (
                      <li
                        key={`${item}-${index}`}
                        className="flex items-start gap-3 rounded-[18px] border border-transparent bg-[#1db954]/10 px-4 py-3"
                      >
                        <span className="mt-1 inline-flex h-7 w-7 items-center justify-center rounded-full border border-[#1db954]/50 text-xs font-semibold text-[#1db954]">
                          {index + 1}
                        </span>
                        <p>{item}</p>
                      </li>
                    ))}
                  </ol>
                ) : (
                  <div className="flex h-40 flex-col items-center justify-center gap-3 text-center text-sm text-white/60">
                    <span className="text-3xl text-[#1db954]">✨</span>
                    
                  </div>
                )}
              </div>
            </section>
          </main>
        </div>
      </div>
    </div>
  );
}
