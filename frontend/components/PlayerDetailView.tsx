'use client';

import { PlayerPrediction } from '@/lib/types';
import { getTeamColors } from '@/lib/teamColors';
import { formatCurrency, formatNumber, getValueCategoryColor } from '@/lib/utils';

interface PlayerDetailViewProps {
  player: PlayerPrediction;
  onBack: () => void;
}

export default function PlayerDetailView({ player, onBack }: PlayerDetailViewProps) {
  const teamColors = getTeamColors(player.team_abbreviation);
  const savings = player.predicted_fmv - player.actual_salary;

  const getSavingsColor = () => {
    if (savings > 0) return 'text-green-700 dark:text-green-400';
    if (savings < 0) return 'text-red-700 dark:text-red-400';
    return 'text-gray-700 dark:text-gray-400';
  };

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <button
          onClick={onBack}
          className="bg-black text-white px-6 py-3 retro-border shadow-retro font-black uppercase text-sm hover:bg-gray-800 transition-colors"
        >
          ← Back
        </button>
      </div>

      <div className="bg-white dark:bg-gray-900 retro-border-thick shadow-retro-lg overflow-hidden">
        <div
          className="p-8 border-b-4 border-black"
          style={{
            background: `linear-gradient(135deg, ${teamColors.primary} 0%, ${teamColors.primary}dd 100%)`,
          }}
        >
          <div className="flex items-center justify-between">
            <div>
              <h2 className="headline-retro text-4xl md:text-6xl text-white mb-2">
                {player.player_name}
              </h2>
              <div className="flex items-center gap-4">
                <div
                  className="inline-block px-4 py-2 retro-border font-black text-lg"
                  style={{
                    backgroundColor: teamColors.secondary,
                    color: teamColors.primary,
                  }}
                >
                  {player.team_abbreviation}
                </div>
                <div className="subhead-retro text-xl text-white/90">
                  {player.position} • Age {player.age}
                </div>
              </div>
            </div>
            <div className="text-right">
              <div
                className={`inline-block px-6 py-3 text-xs font-black uppercase tracking-wide retro-border shadow-retro ${getValueCategoryColor(
                  player.value_category
                )}`}
              >
                {player.value_category}
              </div>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 p-6 bg-gray-50 dark:bg-gray-950">
          <div className="bg-white dark:bg-gray-900 retro-border p-6">
            <div className="text-xs font-black uppercase tracking-widest text-gray-600 dark:text-gray-400 mb-2">
              Predicted FMV
            </div>
            <div className="text-2xl font-black text-black dark:text-white">
              {formatCurrency(player.predicted_fmv)}
            </div>
            <div className="text-xs font-bold text-gray-600 dark:text-gray-400 mt-1">
              {formatNumber(player.predicted_salary_cap_pct, 2)}% of cap
            </div>
          </div>

          <div className="bg-white dark:bg-gray-900 retro-border p-6">
            <div className="text-xs font-black uppercase tracking-widest text-gray-600 dark:text-gray-400 mb-2">
              Actual Salary
            </div>
            <div className="text-2xl font-black text-black dark:text-white">
              {formatCurrency(player.actual_salary)}
            </div>
            <div className="text-xs font-bold text-gray-600 dark:text-gray-400 mt-1">
              {formatNumber(player.actual_salary_cap_pct, 2)}% of cap
            </div>
          </div>

          <div className="bg-white dark:bg-gray-900 retro-border p-6">
            <div className="text-xs font-black uppercase tracking-widest text-gray-600 dark:text-gray-400 mb-2">
              {savings > 0 ? 'Savings' : 'Overspend'}
            </div>
            <div className={`text-2xl font-black ${getSavingsColor()}`}>
              {savings > 0 ? '+' : ''}{formatCurrency(savings)}
            </div>
          </div>

          <div className="bg-white dark:bg-gray-900 retro-border p-6">
            <div className="text-xs font-black uppercase tracking-widest text-gray-600 dark:text-gray-400 mb-2">
              Overpay %
            </div>
            <div className={`text-2xl font-black ${getSavingsColor()}`}>
              {player.inefficiency_score > 0 ? '+' : ''}
              {formatNumber(player.inefficiency_score * 100, 1)}%
            </div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <div className="bg-white dark:bg-gray-900 retro-border-thick shadow-retro-lg">
          <div className="p-6 bg-retro-blue border-b-4 border-black">
            <div className="subhead-retro text-lg text-white">
              PERFORMANCE STATS
            </div>
          </div>
          <div className="p-6 space-y-4">
            <div className="grid grid-cols-3 gap-4">
              <div className="text-center p-4 bg-gray-50 dark:bg-gray-950 retro-border">
                <div className="text-3xl font-black text-black dark:text-white">
                  {formatNumber(player.points, 1)}
                </div>
                <div className="text-xs font-black uppercase tracking-widest text-gray-600 dark:text-gray-400 mt-1">
                  PPG
                </div>
              </div>
              <div className="text-center p-4 bg-gray-50 dark:bg-gray-950 retro-border">
                <div className="text-3xl font-black text-black dark:text-white">
                  {formatNumber(player.rebounds, 1)}
                </div>
                <div className="text-xs font-black uppercase tracking-widest text-gray-600 dark:text-gray-400 mt-1">
                  RPG
                </div>
              </div>
              <div className="text-center p-4 bg-gray-50 dark:bg-gray-950 retro-border">
                <div className="text-3xl font-black text-black dark:text-white">
                  {formatNumber(player.assists, 1)}
                </div>
                <div className="text-xs font-black uppercase tracking-widest text-gray-600 dark:text-gray-400 mt-1">
                  APG
                </div>
              </div>
            </div>

            {player.games_played && (
              <div className="grid grid-cols-2 gap-4">
                <div className="flex justify-between p-3 bg-gray-50 dark:bg-gray-950 retro-border">
                  <span className="text-sm font-bold text-gray-600 dark:text-gray-400 uppercase">
                    Games Played
                  </span>
                  <span className="text-sm font-black text-black dark:text-white">
                    {player.games_played}
                  </span>
                </div>
                {player.games_started !== undefined && (
                  <div className="flex justify-between p-3 bg-gray-50 dark:bg-gray-950 retro-border">
                    <span className="text-sm font-bold text-gray-600 dark:text-gray-400 uppercase">
                      Games Started
                    </span>
                    <span className="text-sm font-black text-black dark:text-white">
                      {player.games_started}
                    </span>
                  </div>
                )}
              </div>
            )}

            {player.minutes !== undefined && (
              <div className="flex justify-between p-3 bg-gray-50 dark:bg-gray-950 retro-border">
                <span className="text-sm font-bold text-gray-600 dark:text-gray-400 uppercase">
                  Minutes Per Game
                </span>
                <span className="text-sm font-black text-black dark:text-white">
                  {formatNumber(player.minutes, 1)}
                </span>
              </div>
            )}

            {(player.steals !== undefined || player.blocks !== undefined) && (
              <div className="grid grid-cols-2 gap-4">
                {player.steals !== undefined && (
                  <div className="flex justify-between p-3 bg-gray-50 dark:bg-gray-950 retro-border">
                    <span className="text-sm font-bold text-gray-600 dark:text-gray-400 uppercase">
                      Steals
                    </span>
                    <span className="text-sm font-black text-black dark:text-white">
                      {formatNumber(player.steals, 1)}
                    </span>
                  </div>
                )}
                {player.blocks !== undefined && (
                  <div className="flex justify-between p-3 bg-gray-50 dark:bg-gray-950 retro-border">
                    <span className="text-sm font-bold text-gray-600 dark:text-gray-400 uppercase">
                      Blocks
                    </span>
                    <span className="text-sm font-black text-black dark:text-white">
                      {formatNumber(player.blocks, 1)}
                    </span>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        <div className="bg-white dark:bg-gray-900 retro-border-thick shadow-retro-lg">
          <div className="p-6 bg-retro-orange border-b-4 border-black">
            <div className="subhead-retro text-lg text-white">
              SHOOTING & ADVANCED METRICS
            </div>
          </div>
          <div className="p-6 space-y-4">
            {(player.fg_pct !== undefined ||
              player.fg3_pct !== undefined ||
              player.ft_pct !== undefined) && (
              <div className="grid grid-cols-3 gap-4">
                {player.fg_pct !== undefined && (
                  <div className="text-center p-4 bg-gray-50 dark:bg-gray-950 retro-border">
                    <div className="text-2xl font-black text-black dark:text-white">
                      {formatNumber(player.fg_pct * 100, 1)}%
                    </div>
                    <div className="text-xs font-black uppercase tracking-widest text-gray-600 dark:text-gray-400 mt-1">
                      FG%
                    </div>
                  </div>
                )}
                {player.fg3_pct !== undefined && (
                  <div className="text-center p-4 bg-gray-50 dark:bg-gray-950 retro-border">
                    <div className="text-2xl font-black text-black dark:text-white">
                      {formatNumber(player.fg3_pct * 100, 1)}%
                    </div>
                    <div className="text-xs font-black uppercase tracking-widest text-gray-600 dark:text-gray-400 mt-1">
                      3P%
                    </div>
                  </div>
                )}
                {player.ft_pct !== undefined && (
                  <div className="text-center p-4 bg-gray-50 dark:bg-gray-950 retro-border">
                    <div className="text-2xl font-black text-black dark:text-white">
                      {formatNumber(player.ft_pct * 100, 1)}%
                    </div>
                    <div className="text-xs font-black uppercase tracking-widest text-gray-600 dark:text-gray-400 mt-1">
                      FT%
                    </div>
                  </div>
                )}
              </div>
            )}

            <div className="space-y-3">
              {player.per !== undefined && (
                <div className="flex justify-between p-3 bg-gray-50 dark:bg-gray-950 retro-border">
                  <span className="text-sm font-bold text-gray-600 dark:text-gray-400 uppercase">
                    PER (Player Efficiency)
                  </span>
                  <span className="text-sm font-black text-black dark:text-white">
                    {formatNumber(player.per, 1)}
                  </span>
                </div>
              )}
              {player.ts_pct !== undefined && (
                <div className="flex justify-between p-3 bg-gray-50 dark:bg-gray-950 retro-border">
                  <span className="text-sm font-bold text-gray-600 dark:text-gray-400 uppercase">
                    TS% (True Shooting)
                  </span>
                  <span className="text-sm font-black text-black dark:text-white">
                    {formatNumber(player.ts_pct * 100, 1)}%
                  </span>
                </div>
              )}
              {player.usg_pct !== undefined && (
                <div className="flex justify-between p-3 bg-gray-50 dark:bg-gray-950 retro-border">
                  <span className="text-sm font-bold text-gray-600 dark:text-gray-400 uppercase">
                    USG% (Usage Rate)
                  </span>
                  <span className="text-sm font-black text-black dark:text-white">
                    {formatNumber(player.usg_pct, 1)}%
                  </span>
                </div>
              )}
              {player.ws !== undefined && (
                <div className="flex justify-between p-3 bg-gray-50 dark:bg-gray-950 retro-border">
                  <span className="text-sm font-bold text-gray-600 dark:text-gray-400 uppercase">
                    WS (Win Shares)
                  </span>
                  <span className="text-sm font-black text-black dark:text-white">
                    {formatNumber(player.ws, 1)}
                  </span>
                </div>
              )}
              {player.bpm !== undefined && (
                <div className="flex justify-between p-3 bg-gray-50 dark:bg-gray-950 retro-border">
                  <span className="text-sm font-bold text-gray-600 dark:text-gray-400 uppercase">
                    BPM (Box Plus/Minus)
                  </span>
                  <span className="text-sm font-black text-black dark:text-white">
                    {player.bpm > 0 ? '+' : ''}
                    {formatNumber(player.bpm, 1)}
                  </span>
                </div>
              )}
              {player.vorp !== undefined && (
                <div className="flex justify-between p-3 bg-gray-50 dark:bg-gray-950 retro-border">
                  <span className="text-sm font-bold text-gray-600 dark:text-gray-400 uppercase">
                    VORP (Value Over Replacement)
                  </span>
                  <span className="text-sm font-black text-black dark:text-white">
                    {formatNumber(player.vorp, 1)}
                  </span>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
