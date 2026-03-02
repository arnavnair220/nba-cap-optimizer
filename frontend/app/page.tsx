'use client';

import { useState, useEffect } from 'react';
import { api, ApiError } from '@/lib/api';
import { PlayerPrediction, PredictionsQueryParams, TeamEfficiency, TeamDetail } from '@/lib/types';
import PlayerTable from '@/components/PlayerTable';
import PlayerFilters from '@/components/PlayerFilters';
import LoadingSpinner from '@/components/LoadingSpinner';
import ErrorMessage from '@/components/ErrorMessage';
import DashboardStats from '@/components/DashboardStats';
import ValueDistributionChart from '@/components/ValueDistributionChart';
import SalaryScatterPlot from '@/components/SalaryScatterPlot';
import Tabs, { TabPanel } from '@/components/Tabs';
import TeamRankings from '@/components/TeamRankings';
import TeamFilters from '@/components/TeamFilters';
import TeamDetailView from '@/components/TeamDetailView';
import PlayerDetailView from '@/components/PlayerDetailView';
import DataFreshnessInfo from '@/components/DataFreshnessInfo';

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
    limit: 500,
    offset: 0,
  });

  const [teams, setTeams] = useState<TeamEfficiency[]>([]);
  const [teamsLoading, setTeamsLoading] = useState(false);
  const [teamsError, setTeamsError] = useState<string | null>(null);
  const [teamSearchQuery, setTeamSearchQuery] = useState('');
  const [teamSortBy, setTeamSortBy] = useState('net_efficiency');
  const [selectedTeamAbbr, setSelectedTeamAbbr] = useState<string | null>(null);
  const [selectedTeamDetail, setSelectedTeamDetail] = useState<TeamDetail | null>(null);
  const [teamDetailLoading, setTeamDetailLoading] = useState(false);

  const [selectedPlayerName, setSelectedPlayerName] = useState<string | null>(null);
  const [selectedPlayerDetail, setSelectedPlayerDetail] = useState<PlayerPrediction | null>(null);
  const [playerDetailLoading, setPlayerDetailLoading] = useState(false);

  const tabs = [
    { id: 'leaderboard', label: 'Leaderboard' },
    { id: 'pulse', label: 'League Overview' },
    { id: 'teams', label: 'Teams' },
    { id: 'about', label: 'About' },
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

  const fetchTeams = async () => {
    try {
      setTeamsLoading(true);
      setTeamsError(null);
      const response = await api.teams.getAll({});
      setTeams(response.teams);
    } catch (err) {
      if (err instanceof ApiError) {
        setTeamsError(`API Error: ${err.message}`);
      } else if (err instanceof Error) {
        setTeamsError(err.message);
      } else {
        setTeamsError('An unknown error occurred');
      }
    } finally {
      setTeamsLoading(false);
    }
  };

  const fetchTeamDetail = async (teamAbbr: string) => {
    try {
      setTeamDetailLoading(true);
      const teamDetail = await api.teams.getTeam(teamAbbr);
      setSelectedTeamDetail(teamDetail);
      setSelectedTeamAbbr(teamAbbr);
    } catch (err) {
      if (err instanceof ApiError) {
        setTeamsError(`API Error: ${err.message}`);
      } else if (err instanceof Error) {
        setTeamsError(err.message);
      } else {
        setTeamsError('An unknown error occurred');
      }
    } finally {
      setTeamDetailLoading(false);
    }
  };

  const handleTeamClick = (teamAbbr: string) => {
    fetchTeamDetail(teamAbbr);
  };

  const handleBackToTeams = () => {
    setSelectedTeamAbbr(null);
    setSelectedTeamDetail(null);
  };

  const fetchPlayerDetail = async (playerName: string) => {
    try {
      setPlayerDetailLoading(true);
      const playerDetail = await api.predictions.getPlayer(playerName);
      setSelectedPlayerDetail(playerDetail);
      setSelectedPlayerName(playerName);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(`API Error: ${err.message}`);
      } else if (err instanceof Error) {
        setError(err.message);
      } else {
        setError('An unknown error occurred');
      }
    } finally {
      setPlayerDetailLoading(false);
    }
  };

  const handlePlayerClick = (playerName: string) => {
    fetchPlayerDetail(playerName);
  };

  const handleBackFromPlayer = () => {
    setSelectedPlayerName(null);
    setSelectedPlayerDetail(null);
  };

  useEffect(() => {
    fetchPlayers();
    fetchTeams(); // Pre-fetch teams data on initial load
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
      case 'worst_value':
        return b.inefficiency_score - a.inefficiency_score;
      case 'dollar_savings':
        return (b.predicted_fmv - b.actual_salary) - (a.predicted_fmv - a.actual_salary);
      case 'dollar_overspend':
        return (a.predicted_fmv - a.actual_salary) - (b.predicted_fmv - b.actual_salary);
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

  const filteredTeams = teams.filter((team) => {
    const matchesSearch =
      team.team_abbreviation?.toLowerCase().includes(teamSearchQuery.toLowerCase()) ||
      team.full_name?.toLowerCase().includes(teamSearchQuery.toLowerCase());
    return matchesSearch;
  });

  const sortedTeams = [...filteredTeams].sort((a, b) => {
    switch (teamSortBy) {
      case 'avg_inefficiency':
        return a.avg_inefficiency_score - b.avg_inefficiency_score;
      case 'net_efficiency':
        return a.net_efficiency - b.net_efficiency;
      case 'bargain_count':
        return b.bargain_count - a.bargain_count;
      case 'overpaid_count':
        return b.overpaid_count - a.overpaid_count;
      default:
        return 0;
    }
  });

  return (
    <main className="min-h-screen">
      <div className="max-w-7xl mx-auto px-6 pt-8 pb-6">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="headline-retro text-4xl md:text-5xl lg:text-6xl text-retro-blue leading-none" style={{
              WebkitTextStroke: '1.5px #FEF5E7',
              textShadow: '-3px -3px 0 #000, 3px -3px 0 #000, -3px 3px 0 #000, 3px 3px 0 #000, -3px 0 0 #000, 3px 0 0 #000, 0 -3px 0 #000, 0 3px 0 #000'
            }}>
              NBA CAP OPTIMIZER
            </h1>
          </div>
          <div className="hidden md:block">
            <div className="text-right bg-retro-orange text-white px-6 py-3 retro-border shadow-retro">
              <div className="text-3xl font-bold">2025-26</div>
              <div className="text-xs uppercase tracking-widest font-black">Season</div>
              <DataFreshnessInfo />
            </div>
          </div>
        </div>

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
              {selectedPlayerDetail && selectedPlayerName ? (
                <>
                  {playerDetailLoading ? (
                    <LoadingSpinner />
                  ) : (
                    <PlayerDetailView player={selectedPlayerDetail} onBack={handleBackFromPlayer} />
                  )}
                </>
              ) : (
                <>
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
                    resultCount={sortedPlayers.length}
                  />

                  <div className="bg-cream retro-border-thick shadow-retro-lg">
                    {sortedPlayers.length === 0 && (
                      <div className="p-12 text-center">
                        <div className="text-2xl font-black uppercase text-black headline-retro">
                          No Players Found
                        </div>
                        <div className="text-sm font-bold text-gray-600 mt-2 uppercase">
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
                        <PlayerTable players={sortedPlayers} showRank={true} onPlayerClick={handlePlayerClick} />
                      </>
                    )}
                  </div>
                </>
              )}
            </TabPanel>

            <TabPanel tabId="pulse" activeTab={activeTab}>
              {selectedPlayerDetail && selectedPlayerName ? (
                <>
                  {playerDetailLoading ? (
                    <LoadingSpinner />
                  ) : (
                    <PlayerDetailView player={selectedPlayerDetail} onBack={handleBackFromPlayer} />
                  )}
                </>
              ) : (
                <>
                  {players.length > 0 && (
                    <>
                  <div className="mb-8">
                    <DashboardStats players={players} />
                  </div>

                  <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
                    <div className="lg:col-span-1">
                      <ValueDistributionChart players={players} />
                    </div>
                    <div className="lg:col-span-2">
                      <SalaryScatterPlot players={players} />
                    </div>
                  </div>

                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mt-8">
                    <div className="bg-cream retro-border shadow-retro p-6 halftone-bg">
                      <div className="flex items-center gap-3 mb-6">
                        <div className="bg-black text-white px-4 py-2 subhead-retro text-sm">
                          TOP BARGAINS
                        </div>
                        <div className="flex-1 h-1 bg-black"></div>
                      </div>
                      <div className="space-y-3">
                        {players
                          .filter((p) => p.value_category === 'Bargain')
                          .sort((a, b) => a.inefficiency_score - b.inefficiency_score)
                          .slice(0, 5)
                          .map((player, idx) => (
                            <div
                              key={player.player_name}
                              className="flex items-center justify-between p-3 bg-green-200 retro-border"
                            >
                              <div>
                                <div className="font-black text-sm uppercase">
                                  {idx + 1}. {player.player_name}
                                </div>
                                <div className="text-xs font-bold text-gray-600">
                                  {player.team_abbreviation} • {player.position}
                                </div>
                              </div>
                              <div className="text-right">
                                <div className="text-sm font-black text-green-700">
                                  {(player.inefficiency_score * 100).toFixed(1)}%
                                </div>
                              </div>
                            </div>
                          ))}
                      </div>
                    </div>

                    <div className="bg-cream retro-border shadow-retro p-6 halftone-bg">
                      <div className="flex items-center gap-3 mb-6">
                        <div className="bg-black text-white px-4 py-2 subhead-retro text-sm">
                          MOST OVERPAID
                        </div>
                        <div className="flex-1 h-1 bg-black"></div>
                      </div>
                      <div className="space-y-3">
                        {players
                          .filter((p) => p.value_category === 'Overpaid')
                          .sort((a, b) => b.inefficiency_score - a.inefficiency_score)
                          .slice(0, 5)
                          .map((player, idx) => (
                            <div
                              key={player.player_name}
                              className="flex items-center justify-between p-3 bg-red-200 retro-border"
                            >
                              <div>
                                <div className="font-black text-sm uppercase">
                                  {idx + 1}. {player.player_name}
                                </div>
                                <div className="text-xs font-bold text-gray-600">
                                  {player.team_abbreviation} • {player.position}
                                </div>
                              </div>
                              <div className="text-right">
                                <div className="text-sm font-black text-red-700">
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
                </>
              )}
            </TabPanel>

            <TabPanel tabId="teams" activeTab={activeTab}>
              {teamsLoading && <LoadingSpinner />}

              {teamsError && !teamsLoading && (
                <div className="p-6">
                  <ErrorMessage message={teamsError} onRetry={fetchTeams} />
                </div>
              )}

              {!teamsLoading && !teamsError && (
                <>
                  {selectedPlayerDetail && selectedPlayerName ? (
                    <>
                      {playerDetailLoading ? (
                        <LoadingSpinner />
                      ) : (
                        <PlayerDetailView player={selectedPlayerDetail} onBack={handleBackFromPlayer} />
                      )}
                    </>
                  ) : selectedTeamDetail && selectedTeamAbbr ? (
                    <>
                      {teamDetailLoading ? (
                        <LoadingSpinner />
                      ) : (
                        <TeamDetailView team={selectedTeamDetail} onBack={handleBackToTeams} onPlayerClick={handlePlayerClick} />
                      )}
                    </>
                  ) : (
                    <>
                      <TeamFilters
                        sortBy={teamSortBy}
                        onSortByChange={setTeamSortBy}
                        searchQuery={teamSearchQuery}
                        onSearchChange={setTeamSearchQuery}
                      />

                      {sortedTeams.length === 0 && teams.length > 0 && (
                        <div className="bg-cream retro-border-thick shadow-retro-lg p-12 text-center">
                          <div className="text-2xl font-black uppercase text-black headline-retro">
                            No Teams Found
                          </div>
                          <div className="text-sm font-bold text-gray-600 mt-2 uppercase">
                            Try adjusting your search
                          </div>
                        </div>
                      )}

                      {sortedTeams.length > 0 && (
                        <TeamRankings teams={sortedTeams} onTeamClick={handleTeamClick} />
                      )}
                    </>
                  )}
                </>
              )}
            </TabPanel>

            <TabPanel tabId="about" activeTab={activeTab}>
              <div className="bg-cream retro-border-thick shadow-retro-lg p-8 halftone-bg">
                <div className="max-w-4xl mx-auto">
                  <h2 className="headline-retro text-3xl text-black mb-6">ABOUT NBA CAP OPTIMIZER</h2>

                  <div className="space-y-6 text-black">
                    <div className="bg-blue-100 p-6 retro-border">
                      <h3 className="subhead-retro text-xl mb-3 text-retro-blue">What is This?</h3>
                      <p className="font-bold leading-relaxed">
                        NBA Cap Optimizer analyzes player performance to predict what each player&apos;s salary should be worth. We then compare these predictions to what teams are actually paying to identify team-friendly bargains and overpaid contracts.
                      </p>
                    </div>

                    <div className="bg-green-100 p-6 retro-border">
                      <h3 className="subhead-retro text-xl mb-3 text-green-700">How It Works</h3>
                      <p className="font-bold leading-relaxed mb-3">
                        Our machine learning models analyze player performance metrics, team statistics, and market trends to calculate what a player&apos;s salary should be based on their production.
                      </p>
                      <ul className="list-disc list-inside space-y-2 font-bold ml-4">
                        <li><span className="text-green-700 font-black">Bargain:</span> Player is producing more than their salary suggests (team-friendly deal)</li>
                        <li><span className="text-yellow-600 font-black">Fair:</span> Player&apos;s salary aligns with their market value</li>
                        <li><span className="text-red-700 font-black">Overpaid:</span> Player is producing less than their salary suggests (team overspending)</li>
                      </ul>
                    </div>

                    <div className="bg-red-100 p-6 retro-border">
                      <h3 className="subhead-retro text-xl mb-3 text-retro-red">Technology Stack</h3>
                      <div className="space-y-3">
                        <p className="font-bold leading-relaxed">
                          <span className="text-retro-red font-black">ML Pipeline:</span> Python-based training and inference using Random Forest models on AWS SageMaker. Automated feature engineering, hyperparameter tuning, and batch transform jobs for predictions.
                        </p>
                        <p className="font-bold leading-relaxed">
                          <span className="text-retro-red font-black">MLOps Infrastructure:</span> Fully automated with AWS Step Functions orchestrating ETL, training, and prediction pipelines. EventBridge schedules for weekly stats updates and model retraining. Model versioning and metadata tracking for reproducibility.
                        </p>
                        <p className="font-bold leading-relaxed">
                          <span className="text-retro-red font-black">Data Engineering:</span> Serverless ETL with AWS Lambda extracting from Basketball Reference, transforming player stats, and loading to PostgreSQL (RDS). S3 for model artifacts and intermediate data storage.
                        </p>
                        <p className="font-bold leading-relaxed">
                          <span className="text-retro-red font-black">Infrastructure as Code:</span> Terraform managing all AWS resources (Lambda, SageMaker, RDS, VPC, API Gateway, IAM, Route 53, CloudFront, Certificate Manager). GitHub Actions CI/CD for automated deployments with approval gates. Modular architecture with separate environments for dev/prod.
                        </p>
                        <p className="font-bold leading-relaxed">
                          <span className="text-retro-red font-black">Frontend & Deployment:</span> Next.js 14 with TypeScript and Tailwind CSS hosted on S3/CloudFront with custom domain via Route 53. SSL/TLS certificates managed through AWS Certificate Manager. RESTful API integration with API Gateway.
                        </p>
                      </div>
                    </div>

                    <div className="bg-yellow-100 p-6 retro-border">
                      <h3 className="subhead-retro text-xl mb-3 text-yellow-700">Data Source & Updates</h3>
                      <p className="font-bold leading-relaxed mb-3">
                        The ML models are trained on multiple years of historical NBA player statistics and salary data sourced from Basketball Reference, ESPN, and official NBA APIs. Current predictions are for the 2025-26 season.
                      </p>
                      <p className="font-bold leading-relaxed mb-2 text-yellow-700">
                        Automated Update Schedule:
                      </p>
                      <ul className="list-disc list-inside space-y-1 font-bold ml-4">
                        <li><span className="font-black">Player Stats:</span> Weekly (every Sunday with latest performance data)</li>
                        <li><span className="font-black">Salaries & Rosters:</span> Monthly (1st of each month with contract updates)</li>
                        <li><span className="font-black">Predictions Regenerated:</span> Weekly (every Sunday) with updated data</li>
                        <li><span className="font-black">Model Retrained:</span> Yearly (during off-season with complete historical data)</li>
                      </ul>
                      <p className="font-bold leading-relaxed mt-3 text-sm text-gray-700">
                        Check the season badge above for the exact dates of the latest updates.
                      </p>
                    </div>

                    <div className="bg-orange-100 p-6 retro-border">
                      <h3 className="subhead-retro text-xl mb-3 text-retro-orange">Developer</h3>
                      <p className="font-bold leading-relaxed mb-3">
                        Created by <span className="text-2xl font-black text-black">Arnav Nair</span>
                      </p>
                      <a
                        href="https://github.com/arnavnair220/nba-cap-optimizer"
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-block bg-black text-white px-4 py-2 retro-border font-bold uppercase text-sm hover:bg-gray-800 transition-colors"
                      >
                        View on GitHub →
                      </a>
                    </div>
                  </div>
                </div>
              </div>
            </TabPanel>
          </>
        )}
      </div>
    </main>
  );
}
