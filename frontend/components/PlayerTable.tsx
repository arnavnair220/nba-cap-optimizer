'use client';

import { useState } from 'react';
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

type SortColumn = 'rank' | 'rank_change' | 'player' | 'team' | 'position' | 'predicted' | 'actual' | 'savings' | 'overpay' | 'value' | null;
type SortDirection = 'asc' | 'desc' | null;

export default function PlayerTable({ players, showRank = true, showTeam = true, onPlayerClick }: PlayerTableProps) {
  const [sortColumn, setSortColumn] = useState<SortColumn>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);

  const handleColumnClick = (column: SortColumn) => {
    if (sortColumn === column) {
      if (sortDirection === 'asc') {
        setSortDirection('desc');
      } else if (sortDirection === 'desc') {
        setSortColumn(null);
        setSortDirection(null);
      }
    } else {
      setSortColumn(column);
      setSortDirection('asc');
    }
  };

  const getSortIndicator = (column: SortColumn) => {
    if (sortColumn !== column) return null;
    if (sortDirection === 'asc') return ' ↑';
    if (sortDirection === 'desc') return ' ↓';
    return null;
  };

  const sortedPlayers = [...players].sort((a, b) => {
    if (!sortColumn || !sortDirection) return 0;

    let compareValue = 0;
    switch (sortColumn) {
      case 'rank':
        compareValue = players.indexOf(a) - players.indexOf(b);
        break;
      case 'rank_change':
        const rankChangeA = a.rank_change ?? 0;
        const rankChangeB = b.rank_change ?? 0;
        compareValue = rankChangeB - rankChangeA;
        break;
      case 'player':
        compareValue = a.player_name.localeCompare(b.player_name);
        break;
      case 'team':
        compareValue = a.team_abbreviation.localeCompare(b.team_abbreviation);
        break;
      case 'position':
        compareValue = a.position.localeCompare(b.position);
        break;
      case 'predicted':
        compareValue = a.predicted_fmv - b.predicted_fmv;
        break;
      case 'actual':
        compareValue = a.actual_salary - b.actual_salary;
        break;
      case 'savings':
        const savingsA = calculateSavings(a);
        const savingsB = calculateSavings(b);
        compareValue = savingsA - savingsB;
        break;
      case 'overpay':
        compareValue = a.inefficiency_score - b.inefficiency_score;
        break;
      case 'value':
        const valueOrder = { 'Bargain': 1, 'Fair': 2, 'Overpaid': 3 };
        compareValue = valueOrder[a.value_category] - valueOrder[b.value_category];
        break;
    }

    return sortDirection === 'asc' ? compareValue : -compareValue;
  });
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y-4 divide-black border-collapse">
        <thead className="bg-black text-white">
          <tr className="border-l-4 border-black">
            {showRank && (
              <>
                <th
                  className="px-3 py-3 text-left text-xs font-black text-retro-yellow uppercase tracking-wider subhead-retro cursor-pointer hover:bg-gray-900 transition-colors"
                  onClick={() => handleColumnClick('rank')}
                >
                  #{getSortIndicator('rank')}
                </th>
                <th
                  className="px-3 py-3 text-center text-xs font-black text-retro-yellow uppercase tracking-wider subhead-retro cursor-pointer hover:bg-gray-900 transition-colors"
                  onClick={() => handleColumnClick('rank_change')}
                >
                  ±{getSortIndicator('rank_change')}
                </th>
              </>
            )}
            <th
              className="px-4 py-3 text-left text-xs font-black uppercase tracking-wider subhead-retro cursor-pointer hover:bg-gray-900 transition-colors"
              onClick={() => handleColumnClick('player')}
            >
              Player{getSortIndicator('player')}
            </th>
            {showTeam && (
              <th
                className="px-4 py-3 text-left text-xs font-black uppercase tracking-wider subhead-retro cursor-pointer hover:bg-gray-900 transition-colors"
                onClick={() => handleColumnClick('team')}
              >
                Team{getSortIndicator('team')}
              </th>
            )}
            <th
              className="px-3 py-3 text-left text-xs font-black uppercase tracking-wider subhead-retro cursor-pointer hover:bg-gray-900 transition-colors"
              onClick={() => handleColumnClick('position')}
            >
              Pos{getSortIndicator('position')}
            </th>
            <th
              className="px-3 py-3 text-right text-xs font-black uppercase tracking-wider subhead-retro cursor-pointer hover:bg-gray-900 transition-colors"
              onClick={() => handleColumnClick('predicted')}
            >
              Predicted{getSortIndicator('predicted')}
            </th>
            <th
              className="px-3 py-3 text-right text-xs font-black uppercase tracking-wider subhead-retro cursor-pointer hover:bg-gray-900 transition-colors"
              onClick={() => handleColumnClick('actual')}
            >
              Actual{getSortIndicator('actual')}
            </th>
            <th
              className="px-3 py-3 text-right text-xs font-black uppercase tracking-wider subhead-retro cursor-pointer hover:bg-gray-900 transition-colors"
              onClick={() => handleColumnClick('savings')}
            >
              $ Diff{getSortIndicator('savings')}
            </th>
            <th
              className="px-3 py-3 text-center text-xs font-black uppercase tracking-wider subhead-retro cursor-pointer hover:bg-gray-900 transition-colors"
              onClick={() => handleColumnClick('overpay')}
            >
              Overpay %{getSortIndicator('overpay')}
            </th>
            <th
              className="px-3 py-3 text-center text-xs font-black uppercase tracking-wider subhead-retro cursor-pointer hover:bg-gray-900 transition-colors"
              onClick={() => handleColumnClick('value')}
            >
              Value{getSortIndicator('value')}
            </th>
          </tr>
        </thead>
        <tbody className="bg-cream divide-y-2 divide-gray-300">
          {sortedPlayers.map((player, index) => {
            const savings = calculateSavings(player);
            const teamColors = getTeamColors(player.team_abbreviation);
            return (
              <tr
                key={`${player.player_name}-${player.season}`}
                className="hover:bg-gray-100 transition-colors border-l-4"
                style={{ borderLeftColor: teamColors.primary }}
              >
                {showRank && (
                  <>
                    <td className="px-3 py-3 whitespace-nowrap">
                      <div className="text-xl font-black text-black headline-retro">
                        {index + 1}
                      </div>
                    </td>
                    <td className="px-3 py-3 whitespace-nowrap text-center">
                      {player.rank_change === null || player.rank_change === undefined ? (
                        <div className="text-gray-400 font-bold text-sm">—</div>
                      ) : player.rank_change === 0 ? (
                        <div className="text-gray-400 font-bold text-sm">—</div>
                      ) : player.rank_change > 0 ? (
                        <div className="text-green-600 font-black text-sm flex items-center justify-center gap-0.5">
                          <span>▲</span>
                          <span>{player.rank_change}</span>
                        </div>
                      ) : (
                        <div className="text-red-600 font-black text-sm flex items-center justify-center gap-0.5">
                          <span>▼</span>
                          <span>{Math.abs(player.rank_change)}</span>
                        </div>
                      )}
                    </td>
                  </>
                )}
                <td className="px-4 py-3 whitespace-nowrap">
                  <div
                    className={`text-sm font-bold text-black uppercase ${
                      onPlayerClick ? 'cursor-pointer hover:text-retro-blue transition-colors' : ''
                    }`}
                    onClick={() => onPlayerClick?.(player.player_name)}
                  >
                    {player.player_name}
                  </div>
                  <div className="text-xs font-semibold text-gray-600">
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
                  <div className="text-sm font-bold text-black uppercase">
                    {player.position}
                  </div>
                </td>
                <td className="px-3 py-3 whitespace-nowrap text-right">
                  <div className="text-sm font-bold text-black">
                    {formatCurrency(player.predicted_fmv)}
                  </div>
                </td>
                <td className="px-3 py-3 whitespace-nowrap text-right">
                  <div className="text-sm font-bold text-black">
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
