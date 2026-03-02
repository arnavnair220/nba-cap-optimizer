'use client';

import { useState } from 'react';
import { TeamEfficiency } from '@/lib/types';

interface TeamRankingsProps {
  teams: TeamEfficiency[];
  onTeamClick: (teamAbbr: string) => void;
}

type SortColumn = 'rank' | 'team' | 'players' | 'payroll' | 'avg_overpay' | 'net_overspend' | 'bargains' | 'fair' | 'overpaid' | null;
type SortDirection = 'asc' | 'desc' | null;

export default function TeamRankings({ teams, onTeamClick }: TeamRankingsProps) {
  const [sortColumn, setSortColumn] = useState<SortColumn>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);
  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  const formatPercentage = (value: number) => {
    return `${(value * 100).toFixed(1)}%`;
  };

  const getEfficiencyColor = (score: number) => {
    if (score < -0.05) return 'text-green-700 dark:text-green-400';
    if (score > 0.05) return 'text-red-700 dark:text-red-400';
    return 'text-gray-700 dark:text-gray-400';
  };

  const getEfficiencyBgColor = (netEfficiency: number) => {
    if (netEfficiency < -5000000) return 'bg-green-50 dark:bg-green-950';
    if (netEfficiency > 5000000) return 'bg-red-50 dark:bg-red-950';
    return 'bg-gray-50 dark:bg-gray-950';
  };

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

  const sortedTeams = [...teams].sort((a, b) => {
    if (!sortColumn || !sortDirection) return 0;

    let compareValue = 0;
    switch (sortColumn) {
      case 'rank':
        compareValue = teams.indexOf(a) - teams.indexOf(b);
        break;
      case 'team':
        compareValue = a.team_abbreviation.localeCompare(b.team_abbreviation);
        break;
      case 'players':
        compareValue = a.player_count - b.player_count;
        break;
      case 'payroll':
        compareValue = a.total_payroll - b.total_payroll;
        break;
      case 'avg_overpay':
        compareValue = a.avg_inefficiency_score - b.avg_inefficiency_score;
        break;
      case 'net_overspend':
        compareValue = a.net_efficiency - b.net_efficiency;
        break;
      case 'bargains':
        compareValue = a.bargain_count - b.bargain_count;
        break;
      case 'fair':
        compareValue = a.fair_count - b.fair_count;
        break;
      case 'overpaid':
        compareValue = a.overpaid_count - b.overpaid_count;
        break;
    }

    return sortDirection === 'asc' ? compareValue : -compareValue;
  });

  return (
    <div className="bg-white dark:bg-gray-900 retro-border-thick shadow-retro-lg overflow-hidden">
      <div className="p-6 bg-retro-blue border-b-4 border-black">
        <div className="subhead-retro text-lg text-white flex items-center justify-between">
          <span>TEAM EFFICIENCY RANKINGS</span>
          <span className="bg-white text-black px-3 py-1 retro-border">
            {teams.length} TEAMS
          </span>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-gray-900 text-white">
            <tr>
              <th
                className="text-left px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black cursor-pointer hover:bg-gray-800 transition-colors"
                onClick={() => handleColumnClick('rank')}
              >
                Rank{getSortIndicator('rank')}
              </th>
              <th
                className="text-left px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black cursor-pointer hover:bg-gray-800 transition-colors"
                onClick={() => handleColumnClick('team')}
              >
                Team{getSortIndicator('team')}
              </th>
              <th
                className="text-right px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black cursor-pointer hover:bg-gray-800 transition-colors"
                onClick={() => handleColumnClick('players')}
              >
                Players{getSortIndicator('players')}
              </th>
              <th
                className="text-right px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black cursor-pointer hover:bg-gray-800 transition-colors"
                onClick={() => handleColumnClick('payroll')}
              >
                Total Payroll{getSortIndicator('payroll')}
              </th>
              <th
                className="text-right px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black cursor-pointer hover:bg-gray-800 transition-colors"
                onClick={() => handleColumnClick('avg_overpay')}
              >
                Avg Overpay %{getSortIndicator('avg_overpay')}
              </th>
              <th
                className="text-right px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black cursor-pointer hover:bg-gray-800 transition-colors"
                onClick={() => handleColumnClick('net_overspend')}
              >
                Net Overspend{getSortIndicator('net_overspend')}
              </th>
              <th
                className="text-center px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black cursor-pointer hover:bg-gray-800 transition-colors"
                onClick={() => handleColumnClick('bargains')}
              >
                Bargains{getSortIndicator('bargains')}
              </th>
              <th
                className="text-center px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black cursor-pointer hover:bg-gray-800 transition-colors"
                onClick={() => handleColumnClick('fair')}
              >
                Fair{getSortIndicator('fair')}
              </th>
              <th
                className="text-center px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black cursor-pointer hover:bg-gray-800 transition-colors"
                onClick={() => handleColumnClick('overpaid')}
              >
                Overpaid{getSortIndicator('overpaid')}
              </th>
            </tr>
          </thead>
          <tbody>
            {sortedTeams.map((team, index) => (
              <tr
                key={team.team_abbreviation}
                onClick={() => onTeamClick(team.team_abbreviation)}
                className={`
                  border-b-2 border-gray-200 dark:border-gray-800 cursor-pointer
                  transition-all hover:bg-gray-100 dark:hover:bg-gray-800
                  ${getEfficiencyBgColor(team.net_efficiency)}
                `}
              >
                <td className="px-6 py-4">
                  <div className="bg-black text-white w-10 h-10 flex items-center justify-center font-black text-lg retro-border">
                    {index + 1}
                  </div>
                </td>
                <td className="px-6 py-4">
                  <div>
                    <div className="font-black text-sm uppercase text-black dark:text-white">
                      {team.team_abbreviation}
                    </div>
                    <div className="text-xs font-bold text-gray-600 dark:text-gray-400">
                      {team.full_name}
                    </div>
                  </div>
                </td>
                <td className="px-6 py-4 text-right">
                  <div className="font-black text-sm text-black dark:text-white">
                    {team.player_count}
                  </div>
                </td>
                <td className="px-6 py-4 text-right">
                  <div className="font-black text-sm text-black dark:text-white">
                    {formatCurrency(team.total_payroll)}
                  </div>
                </td>
                <td className="px-6 py-4 text-right">
                  <div className={`font-black text-sm ${getEfficiencyColor(team.avg_inefficiency_score)}`}>
                    {formatPercentage(team.avg_inefficiency_score)}
                  </div>
                </td>
                <td className="px-6 py-4 text-right">
                  <div className={`font-black text-sm ${getEfficiencyColor(team.net_efficiency / team.total_payroll)}`}>
                    {team.net_efficiency > 0 ? '+' : ''}{formatCurrency(team.net_efficiency)}
                  </div>
                </td>
                <td className="px-6 py-4 text-center">
                  <div className="inline-block bg-green-500 text-white px-3 py-1 retro-border font-black text-xs">
                    {team.bargain_count}
                  </div>
                </td>
                <td className="px-6 py-4 text-center">
                  <div className="inline-block bg-yellow-500 text-white px-3 py-1 retro-border font-black text-xs">
                    {team.fair_count}
                  </div>
                </td>
                <td className="px-6 py-4 text-center">
                  <div className="inline-block bg-red-500 text-white px-3 py-1 retro-border font-black text-xs">
                    {team.overpaid_count}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
