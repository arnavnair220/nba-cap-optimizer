'use client';

import { PlayerPrediction } from '@/lib/types';
import {
  calculateSavings,
  formatCurrency,
  formatNumber,
  formatPercent,
  formatSavings,
  getInefficiencyColor,
  getValueCategoryColor,
} from '@/lib/utils';

interface PlayerTableProps {
  players: PlayerPrediction[];
  showRank?: boolean;
}

export default function PlayerTable({ players, showRank = true }: PlayerTableProps) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
        <thead className="bg-gray-50 dark:bg-gray-800">
          <tr>
            {showRank && (
              <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Rank
              </th>
            )}
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Player
            </th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Team
            </th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Pos
            </th>
            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Predicted FMV
            </th>
            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Actual Salary
            </th>
            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Savings
            </th>
            <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Inefficiency
            </th>
            <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Value
            </th>
            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              VORP
            </th>
          </tr>
        </thead>
        <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
          {players.map((player, index) => {
            const savings = calculateSavings(player);
            return (
              <tr
                key={`${player.player_name}-${player.season}`}
                className="hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
              >
                {showRank && (
                  <td className="px-3 py-4 whitespace-nowrap text-sm font-medium text-gray-900 dark:text-gray-100">
                    {index + 1}
                  </td>
                )}
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="text-sm font-medium text-gray-900 dark:text-gray-100">
                    {player.player_name}
                  </div>
                  <div className="text-sm text-gray-500 dark:text-gray-400">
                    Age {player.age}
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                  {player.team_abbreviation}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                  {player.position}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                  {formatCurrency(player.predicted_fmv)}
                  <div className="text-xs text-gray-500 dark:text-gray-400">
                    {formatPercent(player.predicted_salary_cap_pct)}
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                  {formatCurrency(player.actual_salary)}
                  <div className="text-xs text-gray-500 dark:text-gray-400">
                    {formatPercent(player.actual_salary_cap_pct)}
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-right font-medium">
                  <span className={getInefficiencyColor(player.inefficiency_score)}>
                    {formatSavings(savings)}
                  </span>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-center font-medium">
                  <span className={getInefficiencyColor(player.inefficiency_score)}>
                    {player.inefficiency_score > 0 ? '+' : ''}
                    {formatNumber(player.inefficiency_score * 100, 1)}%
                  </span>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-center">
                  <span
                    className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getValueCategoryColor(
                      player.value_category
                    )}`}
                  >
                    {player.value_category}
                  </span>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900 dark:text-gray-100">
                  {formatNumber(player.vorp)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
