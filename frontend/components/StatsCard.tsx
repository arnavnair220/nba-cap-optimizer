interface StatsCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  trend?: {
    value: number;
    label: string;
  };
  colorClass?: string;
}

export default function StatsCard({ title, value, subtitle, trend, colorClass }: StatsCardProps) {
  const getTrendColor = (value: number) => {
    if (value > 0) return 'text-green-600 dark:text-green-400';
    if (value < 0) return 'text-red-600 dark:text-red-400';
    return 'text-gray-600 dark:text-gray-400';
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
      <div className="text-sm font-medium text-gray-600 dark:text-gray-400 mb-1">{title}</div>
      <div className={`text-3xl font-bold ${colorClass || 'text-gray-900 dark:text-white'} mb-2`}>
        {value}
      </div>
      {subtitle && (
        <div className="text-sm text-gray-500 dark:text-gray-400 mb-2">{subtitle}</div>
      )}
      {trend && (
        <div className={`text-sm font-medium ${getTrendColor(trend.value)}`}>
          {trend.value > 0 ? '+' : ''}
          {trend.value}% {trend.label}
        </div>
      )}
    </div>
  );
}
