'use client';

import { PlayerPrediction } from '@/lib/types';
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine, Label } from 'recharts';

interface SalaryScatterPlotProps {
  players: PlayerPrediction[];
}

export default function SalaryScatterPlot({ players }: SalaryScatterPlotProps) {
  const data = players.map((player) => ({
    name: player.player_name,
    predicted: player.predicted_fmv,
    actual: player.actual_salary,
    category: player.value_category,
  }));

  const getColor = (category: string) => {
    switch (category) {
      case 'Bargain':
        return '#16a34a';
      case 'Fair':
        return '#FFC72C';
      case 'Overpaid':
        return '#E31837';
      default:
        return '#000000';
    }
  };

  const formatCurrency = (value: number) => {
    return `$${(value / 1000000).toFixed(1)}M`;
  };

  return (
    <div className="bg-white dark:bg-gray-900 retro-border shadow-retro p-6 halftone-bg h-full">
      <div className="flex items-center gap-3 mb-6">
        <div className="bg-black text-white px-4 py-2 subhead-retro text-sm">
          MARKET EFFICIENCY
        </div>
        <div className="flex-1 h-1 bg-black"></div>
      </div>
      <ResponsiveContainer width="100%" height={350}>
        <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
          <CartesianGrid strokeDasharray="0" stroke="#000000" strokeWidth={2} />
          <XAxis
            type="number"
            dataKey="predicted"
            name="Predicted FMV"
            stroke="#000000"
            strokeWidth={2}
            tick={{ fill: '#FFFFFF' }}
            tickFormatter={formatCurrency}
            style={{
              fontFamily: 'Arial Black, sans-serif',
              fontWeight: 'bold',
              fontSize: '12px'
            }}
          >
            <Label
              value="PREDICTED FMV"
              position="bottom"
              style={{
                fill: '#FFFFFF',
                fontFamily: 'Arial Black, sans-serif',
                fontWeight: 'bold',
                fontSize: '12px'
              }}
            />
          </XAxis>
          <YAxis
            type="number"
            dataKey="actual"
            name="Actual Salary"
            stroke="#000000"
            strokeWidth={2}
            tick={{ fill: '#FFFFFF' }}
            tickFormatter={formatCurrency}
            style={{
              fontFamily: 'Arial Black, sans-serif',
              fontWeight: 'bold',
              fontSize: '12px'
            }}
          >
            <Label
              value="ACTUAL SALARY"
              angle={-90}
              position="left"
              style={{
                fill: '#FFFFFF',
                fontFamily: 'Arial Black, sans-serif',
                fontWeight: 'bold',
                fontSize: '12px',
                textAnchor: 'middle'
              }}
            />
          </YAxis>
          <Tooltip
            contentStyle={{
              backgroundColor: '#000000',
              border: '4px solid #000000',
              borderRadius: '0',
              fontFamily: 'Arial Black, sans-serif',
              fontWeight: 'bold'
            }}
            labelStyle={{ color: '#FFC72C', fontWeight: 'bold' }}
            itemStyle={{ color: '#FFFFFF', fontWeight: 'bold' }}
            formatter={(value: number, name: string) => [formatCurrency(value), name === 'predicted' ? 'PREDICTED' : 'ACTUAL']}
            cursor={{ strokeDasharray: '3 3' }}
          />
          <ReferenceLine
            stroke="#FFFFFF"
            strokeWidth={3}
            strokeDasharray="5 5"
            segment={[{ x: 0, y: 0 }, { x: 60000000, y: 60000000 }]}
          />
          <Scatter
            name="Players"
            data={data}
            fill="#8884d8"
            shape={(props: any) => {
              const { cx, cy, payload } = props;
              return (
                <circle
                  cx={cx}
                  cy={cy}
                  r={5}
                  fill={getColor(payload.category)}
                  stroke="#000000"
                  strokeWidth={1.5}
                />
              );
            }}
          />
        </ScatterChart>
      </ResponsiveContainer>
      <div className="flex justify-center gap-6 mt-4">
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 bg-green-600 retro-border"></div>
          <span className="text-xs font-black text-black dark:text-white uppercase">Bargain</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 retro-border" style={{ backgroundColor: '#FFC72C' }}></div>
          <span className="text-xs font-black text-black dark:text-white uppercase">Fair</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 retro-border" style={{ backgroundColor: '#E31837' }}></div>
          <span className="text-xs font-black text-black dark:text-white uppercase">Overpaid</span>
        </div>
      </div>
    </div>
  );
}
