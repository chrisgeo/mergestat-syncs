import React, { useEffect, useMemo, useState } from 'react';
import { css } from '@emotion/css';
import { GrafanaTheme2, PanelProps } from '@grafana/data';
import { useStyles2, useTheme2 } from '@grafana/ui';
import { DevHealthOptions } from '../types';
import { frameHasFields, getField, getFieldValue, getFrameWithFields } from './dataFrame';
import { PanelEmptyState } from './PanelEmptyState';

interface Props extends PanelProps<DevHealthOptions> {}

const getStyles = (theme: GrafanaTheme2) => ({
  wrapper: css`
    width: 100%;
    height: 100%;
    overflow: auto;
    font-family: ${theme.typography.fontFamily};
    color: ${theme.colors.text.primary};
  `,
  table: css`
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
    color: ${theme.colors.text.primary};
  `,
  header: css`
    position: sticky;
    top: 0;
    background: ${theme.colors.background.secondary};
    text-align: left;
    font-weight: 600;
  `,
  cell: css`
    padding: 8px 10px;
    border-bottom: 1px solid ${theme.colors.border.weak};
    vertical-align: middle;
  `,
  fileCell: css`
    max-width: 320px;
    word-break: break-all;
    color: ${theme.colors.text.primary};
  `,
  sortButton: css`
    background: none;
    border: none;
    color: inherit;
    font: inherit;
    cursor: pointer;
    padding: 0;
  `,
  muted: css`
    color: ${theme.colors.text.secondary};
    font-style: italic;
  `,
});

const Sparkline: React.FC<{ points: number[] }> = ({ points }) => {
  const theme = useTheme2();
  const styles = useStyles2(getStyles);
  if (points.length === 0) {
    return <span className={styles.muted}>trend unavailable</span>;
  }
  const width = 120;
  const height = 24;
  const max = Math.max(...points);
  const min = Math.min(...points);
  const range = max - min || 1;
  const step = width / Math.max(1, points.length - 1);
  const path = points
    .map((value, index) => {
      const x = index * step;
      const y = height - ((value - min) / range) * height;
      return `${index === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(' ');

  const areaPath = `${path} L${width.toFixed(2)},${height.toFixed(2)} L0,${height.toFixed(2)} Z`;

  const stroke = theme.visualization.getColorByName('blue');

  return (
    <svg width={width} height={height}>
      <path d={areaPath} fill={theme.colors.primary.transparent} fillOpacity={0.3} />
      <path d={path} stroke={stroke} strokeWidth={1.5} fill="none" />
    </svg>
  );
};

const DonutGlyph: React.FC<{ slices: Array<{ label: string; value: number }> }> = ({ slices }) => {
  const theme = useTheme2();
  const styles = useStyles2(getStyles);
  const size = 28;
  const radius = 12;
  const strokeWidth = 6;
  const total = slices.reduce((sum, slice) => sum + slice.value, 0);
  if (total <= 0) {
    return <span className={styles.muted}>n/a</span>;
  }
  let current = 0;
  const driverColors: Record<string, string> = {
    Churn: theme.colors.error.main,
    Complexity: theme.colors.warning.main,
    Ownership: theme.visualization.getColorByName('yellow'),
    Incidents: theme.visualization.getColorByName('purple'),
    Review: theme.visualization.getColorByName('blue'),
  };

  const paths = slices.map((slice) => {
    const value = slice.value;
    if (value <= 0) {
      return null;
    }
    const startAngle = (current / total) * Math.PI * 2;
    const endAngle = ((current + value) / total) * Math.PI * 2;
    current += value;
    const x1 = size / 2 + radius * Math.cos(startAngle);
    const y1 = size / 2 + radius * Math.sin(startAngle);
    const x2 = size / 2 + radius * Math.cos(endAngle);
    const y2 = size / 2 + radius * Math.sin(endAngle);
    const largeArc = endAngle - startAngle > Math.PI ? 1 : 0;

    return (
      <path
        key={slice.label}
        d={`M ${x1} ${y1} A ${radius} ${radius} 0 ${largeArc} 1 ${x2} ${y2}`}
        stroke={driverColors[slice.label] ?? theme.colors.text.secondary}
        strokeWidth={strokeWidth}
        fill="none"
      >
        <title>
          {slice.label}: {((slice.value / total) * 100).toFixed(1)}%
        </title>
      </path>
    );
  });

  return (
    <svg width={size} height={size}>
      <circle
        cx={size / 2}
        cy={size / 2}
        r={radius}
        stroke={theme.colors.border.weak}
        strokeWidth={strokeWidth}
        fill="none"
      />
      {paths}
    </svg>
  );
};

export const HotspotExplorerPanel: React.FC<Props> = ({ data, options }) => {
  const styles = useStyles2(getStyles);
  const tableFrame = getFrameWithFields(data.series, ['file_path', 'churn_loc_30d']);
  const hotspotOptions = options.hotspotExplorer ?? { defaultSortByRisk: true };

  const [sortKey, setSortKey] = useState<'file_path' | 'risk_score'>(
    hotspotOptions.defaultSortByRisk ? 'risk_score' : 'file_path'
  );
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>(
    hotspotOptions.defaultSortByRisk ? 'desc' : 'asc'
  );

  useEffect(() => {
    if (hotspotOptions.defaultSortByRisk) {
      setSortKey('risk_score');
      setSortDir('desc');
    }
  }, [hotspotOptions.defaultSortByRisk]);

  const trendMap = useMemo(() => {
    const map = new Map<string, Array<{ day: string; value: number }>>();
    for (const frame of data.series) {
      if (!frameHasFields(frame, ['file_path', 'day', 'churn_loc'])) {
        continue;
      }
      const fileField = getField(frame, 'file_path');
      const dayField = getField(frame, 'day');
      const valueField = getField(frame, 'churn_loc');
      if (!fileField || !dayField || !valueField) {
        continue;
      }
      const length = frame.length ?? fileField.values.length;
      for (let i = 0; i < length; i++) {
        const filePath = String(getFieldValue<string>(fileField, i) ?? '');
        if (!filePath) {
          continue;
        }
        const day = String(getFieldValue<string>(dayField, i) ?? '');
        const value = Number(getFieldValue<number>(valueField, i));
        if (!Number.isFinite(value)) {
          continue;
        }
        if (!map.has(filePath)) {
          map.set(filePath, []);
        }
        map.get(filePath)?.push({ day, value });
      }
    }
    for (const [key, series] of map.entries()) {
      series.sort((a, b) => a.day.localeCompare(b.day));
      map.set(
        key,
        series.map((point) => ({ ...point, value: point.value }))
      );
    }
    return map;
  }, [data.series]);

  const rows = useMemo(() => {
    if (!tableFrame) {
      return [];
    }
    const fileField = getField(tableFrame, 'file_path');
    if (!fileField) {
      return [];
    }
    const riskField = getField(tableFrame, 'risk_score');
    const churnField = getField(tableFrame, 'churn_loc_30d');
    const complexityField = getField(tableFrame, 'cyclomatic_total');
    const ownershipField = getField(tableFrame, 'ownership_concentration');
    const incidentField = getField(tableFrame, 'incident_count');
    const reviewField = getField(tableFrame, 'review_friction');
    const length = tableFrame.length ?? fileField.values.length;

    const result: Array<{
      filePath: string;
      riskScore?: number;
      churn?: number;
      complexity?: number;
      ownership?: number;
      incidents?: number;
      reviewFriction?: number;
    }> = [];

    for (let i = 0; i < length; i++) {
      const filePath = String(getFieldValue<string>(fileField, i) ?? '');
      if (!filePath) {
        continue;
      }
      result.push({
        filePath,
        riskScore: riskField ? Number(getFieldValue<number>(riskField, i)) : undefined,
        churn: churnField ? Number(getFieldValue<number>(churnField, i)) : undefined,
        complexity: complexityField ? Number(getFieldValue<number>(complexityField, i)) : undefined,
        ownership: ownershipField ? Number(getFieldValue<number>(ownershipField, i)) : undefined,
        incidents: incidentField ? Number(getFieldValue<number>(incidentField, i)) : undefined,
        reviewFriction: reviewField ? Number(getFieldValue<number>(reviewField, i)) : undefined,
      });
    }
    return result;
  }, [tableFrame]);

  if (!tableFrame) {
    return (
      <PanelEmptyState
        title="Hotspot Explorer"
        message="Missing required fields to render the hotspot table."
        schema={[
          'Query A (table facts):',
          '- file_path',
          '- risk_score (optional)',
          '- churn_loc_30d',
          '- cyclomatic_total',
          '- ownership_concentration',
          '- incident_count',
          'Query B (trend):',
          '- file_path',
          '- day',
          '- churn_loc',
        ]}
      />
    );
  }

  const sortedRows = [...rows].sort((a, b) => {
    if (sortKey === 'risk_score') {
      const av = a.riskScore ?? -Infinity;
      const bv = b.riskScore ?? -Infinity;
      return sortDir === 'asc' ? av - bv : bv - av;
    }
    const compare = a.filePath.localeCompare(b.filePath);
    return sortDir === 'asc' ? compare : -compare;
  });

  const toggleSort = (key: 'file_path' | 'risk_score') => {
    if (sortKey === key) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
      return;
    }
    setSortKey(key);
    setSortDir(key === 'risk_score' ? 'desc' : 'asc');
  };

  return (
    <div className={styles.wrapper}>
      <table className={styles.table}>
        <thead className={styles.header}>
          <tr>
            <th className={styles.cell}>
              <button className={styles.sortButton} onClick={() => toggleSort('file_path')} type="button">
                File path
              </button>
            </th>
            <th className={styles.cell}>Trend</th>
            <th className={styles.cell}>Drivers</th>
            <th className={styles.cell}>
              <button className={styles.sortButton} onClick={() => toggleSort('risk_score')} type="button">
                Risk score
              </button>
            </th>
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row) => {
            const trend = trendMap.get(row.filePath) ?? [];
            const sparklinePoints = trend.map((point) => point.value);
            const slices = [
              { label: 'Churn', value: Math.max(0, row.churn ?? 0) },
              { label: 'Complexity', value: Math.max(0, row.complexity ?? 0) },
              { label: 'Ownership', value: Math.max(0, row.ownership ?? 0) },
              { label: 'Incidents', value: Math.max(0, row.incidents ?? 0) },
              { label: 'Review', value: Math.max(0, row.reviewFriction ?? 0) },
            ].filter((slice) => slice.value > 0);

            return (
              <tr key={row.filePath}>
                <td className={`${styles.cell} ${styles.fileCell}`}>{row.filePath}</td>
                <td className={styles.cell}>
                  <Sparkline points={sparklinePoints} />
                </td>
                <td className={styles.cell}>
                  <DonutGlyph slices={slices} />
                </td>
                <td className={styles.cell}>{Number.isFinite(row.riskScore) ? row.riskScore?.toFixed(2) : '—'}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};
