interface StatsCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  trend?: {
    value: number;
    label: string;
  };
  colorClass?: string;
  accent?: 'red' | 'blue' | 'yellow' | 'green';
}

export default function StatsCard({ title, value, subtitle, trend, colorClass, accent = 'red' }: StatsCardProps) {
  const getTrendColor = (value: number) => {
    if (value > 0) return 'text-green-600';
    if (value < 0) return 'text-red-600';
    return 'text-gray-600';
  };

  const accentColors = {
    red: 'bg-retro-red',
    blue: 'bg-retro-blue',
    yellow: 'bg-retro-orange',
    green: 'bg-green-600',
  };

  return (
    <div className="bg-cream retro-border shadow-retro halftone-bg overflow-hidden">
      <div className={`${accentColors[accent]} h-2`}></div>
      <div className="p-6">
        <div className="subhead-retro text-xs text-gray-600 mb-3">
          {title}
        </div>
        <div className={`text-2xl lg:text-3xl xl:text-4xl font-black ${colorClass || 'text-black'} mb-2 headline-retro`}>
          {value}
        </div>
        {subtitle && (
          <div className="text-sm font-bold text-gray-700 uppercase tracking-wide">
            {subtitle}
          </div>
        )}
        {trend && (
          <div className={`text-sm font-bold uppercase mt-2 ${getTrendColor(trend.value)}`}>
            {trend.value > 0 ? '+' : ''}
            {trend.value}% {trend.label}
          </div>
        )}
      </div>
    </div>
  );
}
