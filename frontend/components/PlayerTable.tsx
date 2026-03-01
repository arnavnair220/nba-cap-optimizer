'use client';

import { PlayerPrediction } from '@/lib/types';
import { getTeamColors } from '@/lib/teamColors';
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
  showTeam?: boolean;
  onPlayerClick?: (playerName: string) => void;
}

export default function PlayerTable({ players, showRank = true, showTeam = true, onPlayerClick }: PlayerTableProps) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y-4 divide-black">
        <thead className="bg-black text-white">
          <tr>
            {showRank && (
              <>
                <th className="px-3 py-3 text-left text-xs font-black text-retro-orange uppercase tracking-wider subhead-retro">
                  #
                </th>
                <th className="px-2 py-3 text-center text-xs font-black text-retro-orange uppercase tracking-wider subhead-retro">
                  ±
                </th>
              </>
            )}
            <th className="px-4 py-3 text-left text-xs font-black uppercase tracking-wider subhead-retro">
              Player
            </th>
            {showTeam && (
              <th className="px-4 py-3 text-left text-xs font-black uppercase tracking-wider subhead-retro">
                Team
              </th>
            )}
            <th className="px-3 py-3 text-left text-xs font-black uppercase tracking-wider subhead-retro">
              Pos
            </th>
            <th className="px-3 py-3 text-right text-xs font-black uppercase tracking-wider subhead-retro">
              Predicted
            </th>
            <th className="px-3 py-3 text-right text-xs font-black uppercase tracking-wider subhead-retro">
              Actual
            </th>
            <th className="px-3 py-3 text-right text-xs font-black uppercase tracking-wider subhead-retro">
              Savings
            </th>
            <th className="px-3 py-3 text-center text-xs font-black uppercase tracking-wider subhead-retro">
              Inefficiency
            </th>
            <th className="px-3 py-3 text-center text-xs font-black uppercase tracking-wider subhead-retro">
              Value
            </th>
          </tr>
        </thead>
        <tbody className="bg-white dark:bg-gray-900 divide-y-2 divide-gray-300 dark:divide-gray-700">
          {players.map((player, index) => {
            const savings = calculateSavings(player);
            const teamColors = getTeamColors(player.team_abbreviation);
            return (
              <tr
                key={`${player.player_name}-${player.season}`}
                className="hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors border-l-4"
                style={{ borderLeftColor: teamColors.primary }}
              >
                {showRank && (
                  <>
                    <td className="px-3 py-3 whitespace-nowrap">
                      <div className="text-xl font-black text-black dark:text-white headline-retro">
                        {index + 1}
                      </div>
                    </td>
                    <td className="px-2 py-3 whitespace-nowrap text-center">
                      {player.rank_change === null || player.rank_change === undefined ? (
                        <div className="text-gray-400 dark:text-gray-600 font-bold text-sm">—</div>
                      ) : player.rank_change === 0 ? (
                        <div className="text-gray-400 dark:text-gray-600 font-bold text-sm">—</div>
                      ) : player.rank_change > 0 ? (
                        <div className="text-green-600 dark:text-green-400 font-black text-sm flex items-center justify-center gap-0.5">
                          <span>↑</span>
                          <span>{player.rank_change}</span>
                        </div>
                      ) : (
                        <div className="text-red-600 dark:text-red-400 font-black text-sm flex items-center justify-center gap-0.5">
                          <span>↓</span>
                          <span>{Math.abs(player.rank_change)}</span>
                        </div>
                      )}
                    </td>
                  </>
                )}
                <td className="px-4 py-3 whitespace-nowrap">
                  <div
                    className={`text-sm font-bold text-black dark:text-white uppercase ${
                      onPlayerClick ? 'cursor-pointer hover:text-retro-blue transition-colors' : ''
                    }`}
                    onClick={() => onPlayerClick?.(player.player_name)}
                  >
                    {player.player_name}
                  </div>
                  <div className="text-xs font-semibold text-gray-600 dark:text-gray-400">
                    Age {player.age}
                  </div>
                </td>
                {showTeam && (
                  <td className="px-4 py-4 whitespace-nowrap">
                    <div
                      className="flex items-center justify-center w-14 px-2 py-1 font-black text-sm uppercase tracking-wide retro-border"
                      style={{
                        backgroundColor: teamColors.primary,
                        color: teamColors.secondary
                      }}
                    >
                      {player.team_abbreviation}
                    </div>
                  </td>
                )}
                <td className="px-3 py-3 whitespace-nowrap">
                  <div className="text-sm font-bold text-black dark:text-white uppercase">
                    {player.position}
                  </div>
                </td>
                <td className="px-3 py-3 whitespace-nowrap text-right">
                  <div className="text-sm font-bold text-black dark:text-white">
                    {formatCurrency(player.predicted_fmv)}
                  </div>
                </td>
                <td className="px-3 py-3 whitespace-nowrap text-right">
                  <div className="text-sm font-bold text-black dark:text-white">
                    {formatCurrency(player.actual_salary)}
                  </div>
                </td>
                <td className="px-3 py-3 whitespace-nowrap text-right">
                  <span className={`text-sm font-black ${getInefficiencyColor(player.inefficiency_score)}`}>
                    {formatSavings(savings)}
                  </span>
                </td>
                <td className="px-3 py-3 whitespace-nowrap text-center">
                  <div className={`inline-block px-2 py-1 text-sm font-black ${getInefficiencyColor(player.inefficiency_score)}`}>
                    {player.inefficiency_score > 0 ? '+' : ''}
                    {formatNumber(player.inefficiency_score * 100, 1)}%
                  </div>
                </td>
                <td className="px-3 py-3 whitespace-nowrap text-center">
                  <span
                    className={`inline-block px-2 py-1 text-xs font-black uppercase tracking-wide retro-border ${getValueCategoryColor(
                      player.value_category
                    )}`}
                  >
                    {player.value_category}
                  </span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
