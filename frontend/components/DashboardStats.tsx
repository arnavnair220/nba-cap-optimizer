'use client';

import { PlayerPrediction } from '@/lib/types';
import { formatCurrency } from '@/lib/utils';
import StatsCard from './StatsCard';

interface DashboardStatsProps {
  players: PlayerPrediction[];
}

export default function DashboardStats({ players }: DashboardStatsProps) {
  const bargainCount = players.filter((p) => p.value_category === 'Bargain').length;
  const overpaidCount = players.filter((p) => p.value_category === 'Overpaid').length;

  const totalPredictedValue = players.reduce((sum, p) => sum + p.predicted_fmv, 0);
  const totalActualSalary = players.reduce((sum, p) => sum + p.actual_salary, 0);
  const totalSavings = totalPredictedValue - totalActualSalary;

  const avgInefficiency =
    players.length > 0
      ? players.reduce((sum, p) => sum + p.inefficiency_score, 0) / players.length
      : 0;

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
      <StatsCard
        title="Total Players"
        value={players.length}
        subtitle={`${bargainCount} bargains, ${overpaidCount} overpaid`}
        accent="blue"
      />

      <StatsCard
        title="Total Market Inefficiency"
        value={formatCurrency(Math.abs(totalSavings))}
        subtitle={totalSavings < 0 ? 'Underspending' : 'Overspending'}
        colorClass={
          totalSavings < 0
            ? 'text-green-600 dark:text-green-400'
            : 'text-red-600 dark:text-red-400'
        }
        accent={totalSavings < 0 ? 'green' : 'red'}
      />

      <StatsCard
        title="Avg Inefficiency Score"
        value={`${(avgInefficiency * 100).toFixed(1)}%`}
        subtitle={
          avgInefficiency < -0.05
            ? 'Market underpaying'
            : avgInefficiency > 0.05
              ? 'Market overpaying'
              : 'Market balanced'
        }
        colorClass={
          avgInefficiency < 0
            ? 'text-green-600 dark:text-green-400'
            : avgInefficiency > 0
              ? 'text-red-600 dark:text-red-400'
              : 'text-yellow-600 dark:text-yellow-400'
        }
        accent="yellow"
      />

      <StatsCard
        title="Top Bargain"
        value={
          bargainCount > 0
            ? players.find((p) => p.value_category === 'Bargain')?.player_name || 'N/A'
            : 'N/A'
        }
        subtitle={
          bargainCount > 0
            ? `${((players.find((p) => p.value_category === 'Bargain')?.inefficiency_score || 0) * 100).toFixed(1)}% undervalued`
            : undefined
        }
        accent="green"
      />
    </div>
  );
}
