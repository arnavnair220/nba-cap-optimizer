'use client';

import { TeamDetail } from '@/lib/types';
import PlayerTable from './PlayerTable';
import { getTeamColors } from '@/lib/teamColors';
import { getPlayersCurrentTeamOnly } from '@/lib/utils';

interface TeamDetailViewProps {
  team: TeamDetail;
  onBack: () => void;
  onPlayerClick?: (playerName: string) => void;
}

export default function TeamDetailView({ team, onBack, onPlayerClick }: TeamDetailViewProps) {
  const teamColors = getTeamColors(team.team_abbreviation);
  const currentTeamPlayers = getPlayersCurrentTeamOnly(team.players);

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
    if (score < -0.05) return 'text-green-700';
    if (score > 0.05) return 'text-red-700';
    return 'text-gray-700';
  };

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <button
          onClick={onBack}
          className="bg-black text-white px-6 py-3 retro-border shadow-retro font-black uppercase text-sm hover:bg-gray-800 transition-colors"
        >
          ← Back to Teams
        </button>
      </div>

      <div className="bg-cream retro-border-thick shadow-retro-lg overflow-hidden">
        <div
          className="p-8 border-b-4 border-black"
          style={{ backgroundColor: teamColors.primary }}
        >
          <div className="flex items-center justify-between">
            <div>
              <h2
                className="headline-retro text-4xl md:text-5xl mb-2"
                style={{ color: teamColors.secondary }}
              >
                {team.team_abbreviation}
              </h2>
              <div
                className="subhead-retro text-xl"
                style={{ color: teamColors.secondary, opacity: 0.9 }}
              >
                {team.full_name}
              </div>
            </div>
            <div className="text-right">
              <div
                className="px-6 py-3 retro-border shadow-retro"
                style={{
                  backgroundColor: teamColors.secondary,
                  color: teamColors.primary,
                }}
              >
                <div className="text-3xl font-black">{team.player_count}</div>
                <div className="text-xs uppercase tracking-widest font-black">Players</div>
              </div>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 p-6 bg-gray-50">
          <div className="bg-cream retro-border p-6">
            <div className="text-xs font-black uppercase tracking-widest text-gray-600 mb-2">
              Total Payroll
            </div>
            <div className="text-2xl font-black text-black">
              {formatCurrency(team.total_payroll)}
            </div>
          </div>

          <div className="bg-cream retro-border p-6">
            <div className="text-xs font-black uppercase tracking-widest text-gray-600 mb-2">
              Net Overspend
            </div>
            <div className={`text-2xl font-black ${getEfficiencyColor(team.net_efficiency / team.total_payroll)}`}>
              {team.net_efficiency > 0 ? '+' : ''}{formatCurrency(team.net_efficiency)}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-3 gap-4 p-6 border-t-4 border-black">
          <div className="text-center p-4 bg-green-50 retro-border">
            <div className="text-4xl font-black text-green-700">
              {team.bargain_count}
            </div>
            <div className="text-xs font-black uppercase tracking-widest text-gray-600 mt-2">
              Bargain Contracts
            </div>
          </div>

          <div className="text-center p-4 bg-yellow-50 retro-border">
            <div className="text-4xl font-black text-yellow-700">
              {team.fair_count}
            </div>
            <div className="text-xs font-black uppercase tracking-widest text-gray-600 mt-2">
              Fair Contracts
            </div>
          </div>

          <div className="text-center p-4 bg-red-50 retro-border">
            <div className="text-4xl font-black text-red-700">
              {team.overpaid_count}
            </div>
            <div className="text-xs font-black uppercase tracking-widest text-gray-600 mt-2">
              Overpaid Contracts
            </div>
          </div>
        </div>
      </div>

      <div className="bg-cream retro-border-thick shadow-retro-lg">
        <div className="p-6 bg-retro-orange border-b-4 border-black">
          <div className="subhead-retro text-lg text-white flex items-center justify-between">
            <span>TEAM ROSTER</span>
            <span className="bg-white text-black px-3 py-1 retro-border">
              {currentTeamPlayers.length} PLAYERS
            </span>
          </div>
        </div>
        <PlayerTable players={currentTeamPlayers} showRank={false} showTeam={false} onPlayerClick={onPlayerClick} />
      </div>
    </div>
  );
}
