'use client';

import { PlayerPrediction } from '@/lib/types';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

interface ValueDistributionChartProps {
  players: PlayerPrediction[];
}

export default function ValueDistributionChart({ players }: ValueDistributionChartProps) {
  const bargains = players.filter((p) => p.value_category === 'Bargain').length;
  const fair = players.filter((p) => p.value_category === 'Fair').length;
  const overpaid = players.filter((p) => p.value_category === 'Overpaid').length;

  const data = [
    {
      category: 'Bargain',
      count: bargains,
      fill: '#16a34a',
    },
    {
      category: 'Fair',
      count: fair,
      fill: '#eab308',
    },
    {
      category: 'Overpaid',
      count: overpaid,
      fill: '#dc2626',
    },
  ];

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
      <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
        Value Category Distribution
      </h2>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="category" stroke="#9ca3af" />
          <YAxis stroke="#9ca3af" />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1f2937',
              border: '1px solid #374151',
              borderRadius: '0.5rem',
            }}
            labelStyle={{ color: '#f3f4f6' }}
          />
          <Legend />
          <Bar dataKey="count" fill="#3b82f6" name="Player Count" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
