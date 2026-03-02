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
      category: 'BARGAIN',
      count: bargains,
      fill: '#16a34a',
    },
    {
      category: 'FAIR',
      count: fair,
      fill: '#FFC72C',
    },
    {
      category: 'OVERPAID',
      count: overpaid,
      fill: '#E31837',
    },
  ];

  return (
    <div className="bg-cream retro-border shadow-retro p-6 halftone-bg h-full">
      <div className="flex items-center gap-3 mb-6">
        <div className="bg-black text-white px-4 py-2 subhead-retro text-sm">
          VALUE BREAKDOWN
        </div>
        <div className="flex-1 h-1 bg-black"></div>
      </div>
      <ResponsiveContainer width="100%" height={350}>
        <BarChart data={data} barSize={80}>
          <CartesianGrid strokeDasharray="0" stroke="#000000" strokeWidth={2} />
          <XAxis
            dataKey="category"
            stroke="#000000"
            strokeWidth={2}
            tick={{ fill: '#000000' }}
            style={{
              fontFamily: 'Arial Black, sans-serif',
              fontWeight: 'bold',
              fontSize: '14px'
            }}
          />
          <YAxis
            stroke="#000000"
            strokeWidth={2}
            tick={{ fill: '#000000' }}
            style={{
              fontFamily: 'Arial Black, sans-serif',
              fontWeight: 'bold'
            }}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: '#000000',
              border: '4px solid #000000',
              borderRadius: '0',
              fontFamily: 'Arial Black, sans-serif',
              fontWeight: 'bold'
            }}
            labelStyle={{ color: '#FFC72C', fontWeight: 'bold', textTransform: 'uppercase' }}
            itemStyle={{ color: '#FFFFFF', fontWeight: 'bold' }}
          />
          <Bar dataKey="count" name="PLAYERS" radius={0} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
