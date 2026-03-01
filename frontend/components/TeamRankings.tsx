'use client';

import { TeamEfficiency } from '@/lib/types';

interface TeamRankingsProps {
  teams: TeamEfficiency[];
  onTeamClick: (teamAbbr: string) => void;
}

export default function TeamRankings({ teams, onTeamClick }: TeamRankingsProps) {
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

  const getEfficiencyBgColor = (score: number) => {
    if (score < -0.05) return 'bg-green-50 dark:bg-green-950';
    if (score > 0.05) return 'bg-red-50 dark:bg-red-950';
    return 'bg-gray-50 dark:bg-gray-950';
  };

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
              <th className="text-left px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black">
                Rank
              </th>
              <th className="text-left px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black">
                Team
              </th>
              <th className="text-right px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black">
                Players
              </th>
              <th className="text-right px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black">
                Total Payroll
              </th>
              <th className="text-right px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black">
                Avg Efficiency
              </th>
              <th className="text-right px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black">
                Net Overspend
              </th>
              <th className="text-center px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black">
                Bargains
              </th>
              <th className="text-center px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black">
                Fair
              </th>
              <th className="text-center px-6 py-4 font-black uppercase text-xs tracking-wider border-b-2 border-black">
                Overpaid
              </th>
            </tr>
          </thead>
          <tbody>
            {teams.map((team, index) => (
              <tr
                key={team.team_abbreviation}
                onClick={() => onTeamClick(team.team_abbreviation)}
                className={`
                  border-b-2 border-gray-200 dark:border-gray-800 cursor-pointer
                  transition-all hover:bg-gray-100 dark:hover:bg-gray-800
                  ${getEfficiencyBgColor(team.avg_inefficiency_score)}
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
