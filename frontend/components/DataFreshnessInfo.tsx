'use client';

import { useEffect, useState } from 'react';
import { api } from '@/lib/api';
import { Metadata } from '@/lib/types';

function parseEtlRunId(etlRunId: string | null): Date | null {
  if (!etlRunId) return null;

  const match = etlRunId.match(/^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})$/);
  if (!match) return null;

  const [_, year, month, day, hour, minute, second] = match;
  return new Date(
    parseInt(year),
    parseInt(month) - 1,
    parseInt(day),
    parseInt(hour),
    parseInt(minute),
    parseInt(second)
  );
}

function formatDate(dateString: string | null): string {
  if (!dateString) return 'N/A';

  try {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    });
  } catch {
    return 'N/A';
  }
}

export default function DataFreshnessInfo() {
  const [metadata, setMetadata] = useState<Metadata | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchMetadata = async () => {
      try {
        setLoading(true);
        setError(null);
        const data = await api.metadata.get();
        setMetadata(data);
      } catch (err) {
        console.error('Failed to fetch metadata:', err);
        setError('Failed to load data freshness info');
      } finally {
        setLoading(false);
      }
    };

    fetchMetadata();
  }, []);

  if (loading) {
    return (
      <div className="mt-3 pt-3 border-t border-white/30">
        <div className="text-xs text-white/70">Loading...</div>
      </div>
    );
  }

  if (error || !metadata) {
    return (
      <div className="mt-3 pt-3 border-t border-white/30 space-y-1">
        <div className="text-xs text-white/90">
          <span className="font-semibold">Stats Last Fetched:</span> N/A
        </div>

        <div className="text-xs text-white/90">
          <span className="font-semibold">Predictions Regenerated:</span> N/A
        </div>

        <div className="text-xs text-white/90">
          <span className="font-semibold">Model Retrained:</span> N/A
        </div>
      </div>
    );
  }

  const etlDate = metadata.latest_etl_run_id
    ? parseEtlRunId(metadata.latest_etl_run_id)
    : null;

  return (
    <div className="mt-3 pt-3 border-t border-white/30 space-y-1">
      <div className="text-xs text-white/90">
        <span className="font-semibold">Stats Last Fetched:</span>{' '}
        {etlDate ? formatDate(etlDate.toISOString()) : 'N/A'}
      </div>

      <div className="text-xs text-white/90">
        <span className="font-semibold">Predictions Regenerated:</span>{' '}
        {formatDate(metadata.latest_prediction_date)}
      </div>

      <div className="text-xs text-white/90">
        <span className="font-semibold">Model Retrained:</span>{' '}
        {formatDate(metadata.model_trained_at)}
        {metadata.model_version && (
          <span className="text-white/70"> ({metadata.model_version})</span>
        )}
      </div>
    </div>
  );
}
