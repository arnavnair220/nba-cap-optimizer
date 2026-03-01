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

export default function Home() {
  const [players, setPlayers] = useState<PlayerPrediction[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<PredictionsQueryParams>({
    sort_by: 'inefficiency_score',
    limit: 100,
    offset: 0,
  });

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

  return (
    <main className="min-h-screen p-8 bg-gray-50 dark:bg-gray-900">
      <div className="max-w-7xl mx-auto">
        <header className="mb-8">
          <h1 className="text-4xl font-bold text-gray-900 dark:text-white mb-2">
            NBA Cap Optimizer
          </h1>
          <p className="text-gray-600 dark:text-gray-400">
            Player Value Leaderboard - Identifying Undervalued & Overvalued Contracts
          </p>
        </header>

        {loading && <LoadingSpinner />}

        {error && !loading && (
          <div className="p-6">
            <ErrorMessage message={error} onRetry={fetchPlayers} />
          </div>
        )}

        {!loading && !error && (
          <>
            {players.length > 0 && <DashboardStats players={players} />}

            {players.length > 0 && <ValueDistributionChart players={players} />}

            <PlayerFilters filters={filters} onFilterChange={setFilters} />

            <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
              {players.length === 0 && (
                <div className="p-6 text-center text-gray-600 dark:text-gray-400">
                  No players found matching your filters.
                </div>
              )}

              {players.length > 0 && (
                <>
                  <div className="p-4 border-b border-gray-200 dark:border-gray-700">
                    <div className="text-sm text-gray-600 dark:text-gray-400">
                      Showing {players.length} players
                    </div>
                  </div>
                  <PlayerTable players={players} showRank={true} />
                </>
              )}
            </div>
          </>
        )}
      </div>
    </main>
  );
}
