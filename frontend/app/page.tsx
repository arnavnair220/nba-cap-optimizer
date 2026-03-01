'use client';

import { useState, useEffect } from 'react';
import { api, ApiError } from '@/lib/api';
import { PlayerPrediction, PredictionsQueryParams } from '@/lib/types';
import PlayerTable from '@/components/PlayerTable';
import PlayerFilters from '@/components/PlayerFilters';
import LoadingSpinner from '@/components/LoadingSpinner';
import ErrorMessage from '@/components/ErrorMessage';
import DashboardStats from '@/components/DashboardStats';
import ValueDistributionChart from '@/components/ValueDistributionChart';
import Tabs, { TabPanel } from '@/components/Tabs';

export default function Home() {
  const [players, setPlayers] = useState<PlayerPrediction[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState('leaderboard');
  const [searchQuery, setSearchQuery] = useState('');
  const [team, setTeam] = useState('');
  const [position, setPosition] = useState('');
  const [valueCategory, setValueCategory] = useState('');
  const [sortBy, setSortBy] = useState('inefficiency_score');
  const [filters, setFilters] = useState<PredictionsQueryParams>({
    limit: 100,
    offset: 0,
  });

  const tabs = [
    { id: 'leaderboard', label: 'Leaderboard' },
    { id: 'pulse', label: 'League Pulse' },
  ];

  const fetchPlayers = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await api.predictions.getAll(filters);
      setPlayers(response.predictions);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(`API Error: ${err.message}`);
      } else if (err instanceof Error) {
        setError(err.message);
      } else {
        setError('An unknown error occurred');
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPlayers();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters]);

  const filteredPlayers = players.filter((player) => {
    const matchesSearch = player.player_name.toLowerCase().includes(searchQuery.toLowerCase());
    const matchesTeam = !team || player.team_abbreviation === team;
    const matchesPosition = !position || player.position === position;
    const matchesValueCategory = !valueCategory || player.value_category === valueCategory;

    return matchesSearch && matchesTeam && matchesPosition && matchesValueCategory;
  });

  const sortedPlayers = [...filteredPlayers].sort((a, b) => {
    switch (sortBy) {
      case 'inefficiency_score':
        return a.inefficiency_score - b.inefficiency_score;
      case 'predicted_fmv':
        return b.predicted_fmv - a.predicted_fmv;
      case 'actual_salary':
        return b.actual_salary - a.actual_salary;
      case 'player_name':
        return a.player_name.localeCompare(b.player_name);
      default:
        return 0;
    }
  });

  return (
    <main className="min-h-screen bg-gray-200 dark:bg-gray-900 bg-retro-bold-stripes">
      {/* Retro Header Banner */}
      <div className="bg-white dark:bg-gray-900 retro-border-thick border-b-0 relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-r from-retro-blue/10 via-retro-red/10 to-retro-orange/10"></div>
        <div className="max-w-7xl mx-auto px-6 py-8 relative z-10">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="headline-retro text-6xl md:text-7xl lg:text-8xl text-black dark:text-white mb-2 leading-none">
                CAP OPTIMIZER
              </h1>
              <div className="subhead-retro text-xl md:text-2xl flex items-center gap-4">
                <span className="bg-retro-red text-white px-3 py-1 retro-border">PLAYER VALUE</span>
                <span className="text-black dark:text-white font-black">/</span>
                <span className="bg-retro-blue text-white px-3 py-1 retro-border">CONTRACT ANALYSIS</span>
              </div>
            </div>
            <div className="hidden md:block">
              <div className="text-right bg-retro-orange text-white px-6 py-3 retro-border shadow-retro">
                <div className="text-4xl font-bold">2025-26</div>
                <div className="text-sm uppercase tracking-widest font-black">Season</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-6 py-8">

        {loading && <LoadingSpinner />}

        {error && !loading && (
          <div className="p-6">
            <ErrorMessage message={error} onRetry={fetchPlayers} />
          </div>
        )}

        {!loading && !error && (
          <>
            <Tabs tabs={tabs} activeTab={activeTab} onTabChange={setActiveTab} />

            <TabPanel tabId="leaderboard" activeTab={activeTab}>
              <PlayerFilters
                filters={filters}
                onFilterChange={setFilters}
                searchQuery={searchQuery}
                onSearchChange={setSearchQuery}
                team={team}
                onTeamChange={setTeam}
                position={position}
                onPositionChange={setPosition}
                valueCategory={valueCategory}
                onValueCategoryChange={setValueCategory}
                sortBy={sortBy}
                onSortByChange={setSortBy}
              />

              <div className="bg-white dark:bg-gray-900 retro-border-thick shadow-retro-lg">
                {sortedPlayers.length === 0 && (
                  <div className="p-12 text-center">
                    <div className="text-2xl font-black uppercase text-black dark:text-white headline-retro">
                      No Players Found
                    </div>
                    <div className="text-sm font-bold text-gray-600 dark:text-gray-400 mt-2 uppercase">
                      Try adjusting your filters
                    </div>
                  </div>
                )}

                {sortedPlayers.length > 0 && (
                  <>
                    <div className="p-6 bg-retro-blue border-b-4 border-black">
                      <div className="subhead-retro text-lg text-white flex items-center justify-between">
                        <span>PLAYER RANKINGS</span>
                        <span className="bg-white text-black px-3 py-1 retro-border">
                          {sortedPlayers.length} PLAYERS
                        </span>
                      </div>
                    </div>
                    <PlayerTable players={sortedPlayers} showRank={true} />
                  </>
                )}
              </div>
            </TabPanel>

            <TabPanel tabId="pulse" activeTab={activeTab}>
              {players.length > 0 && (
                <>
                  <div className="mb-8">
                    <div className="flex items-center gap-3 mb-6">
                      <div className="bg-retro-red text-white px-4 py-2 subhead-retro text-sm retro-border">
                        LEAGUE OVERVIEW
                      </div>
                      <div className="flex-1 h-1 bg-retro-red"></div>
                    </div>
                    <DashboardStats players={players} />
                  </div>

                  <ValueDistributionChart players={players} />

                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mt-8">
                    <div className="bg-white dark:bg-gray-900 retro-border shadow-retro p-6 halftone-bg">
                      <div className="flex items-center gap-3 mb-6">
                        <div className="bg-black text-white px-4 py-2 subhead-retro text-sm">
                          TOP BARGAINS
                        </div>
                        <div className="flex-1 h-1 bg-black"></div>
                      </div>
                      <div className="space-y-3">
                        {players
                          .filter((p) => p.value_category === 'Bargain')
                          .slice(0, 5)
                          .map((player, idx) => (
                            <div
                              key={player.player_name}
                              className="flex items-center justify-between p-3 bg-green-50 dark:bg-green-950 retro-border"
                            >
                              <div>
                                <div className="font-black text-sm uppercase">
                                  {idx + 1}. {player.player_name}
                                </div>
                                <div className="text-xs font-bold text-gray-600 dark:text-gray-400">
                                  {player.team_abbreviation} • {player.position}
                                </div>
                              </div>
                              <div className="text-right">
                                <div className="text-sm font-black text-green-700 dark:text-green-400">
                                  {(player.inefficiency_score * 100).toFixed(1)}%
                                </div>
                              </div>
                            </div>
                          ))}
                      </div>
                    </div>

                    <div className="bg-white dark:bg-gray-900 retro-border shadow-retro p-6 halftone-bg">
                      <div className="flex items-center gap-3 mb-6">
                        <div className="bg-black text-white px-4 py-2 subhead-retro text-sm">
                          MOST OVERPAID
                        </div>
                        <div className="flex-1 h-1 bg-black"></div>
                      </div>
                      <div className="space-y-3">
                        {players
                          .filter((p) => p.value_category === 'Overpaid')
                          .slice(0, 5)
                          .map((player, idx) => (
                            <div
                              key={player.player_name}
                              className="flex items-center justify-between p-3 bg-red-50 dark:bg-red-950 retro-border"
                            >
                              <div>
                                <div className="font-black text-sm uppercase">
                                  {idx + 1}. {player.player_name}
                                </div>
                                <div className="text-xs font-bold text-gray-600 dark:text-gray-400">
                                  {player.team_abbreviation} • {player.position}
                                </div>
                              </div>
                              <div className="text-right">
                                <div className="text-sm font-black text-red-700 dark:text-red-400">
                                  +{(player.inefficiency_score * 100).toFixed(1)}%
                                </div>
                              </div>
                            </div>
                          ))}
                      </div>
                    </div>
                  </div>
                </>
              )}
            </TabPanel>
          </>
        )}
      </div>
    </main>
  );
}
