import React, { useMemo } from 'react';
import { css } from '@emotion/css';
import { PanelProps } from '@grafana/data';
import { DevHealthOptions } from '../types';
import { getField, getFieldValue, getFrameWithFields } from './dataFrame';
import { PanelEmptyState } from './PanelEmptyState';

interface Props extends PanelProps<DevHealthOptions> {}

const styles = {
  wrapper: css`
    width: 100%;
    height: 100%;
    font-family: Open Sans, Helvetica, Arial, sans-serif;
  `,
  label: css`
    font-size: 11px;
    fill: #dbe2ea;
  `,
  axisLabel: css`
    font-size: 11px;
    fill: #9aa7b2;
    text-anchor: middle;
    font-weight: 500;
  `,
};

const palette = [
  '#73BF69', // Green
  '#F2CC0C', // Yellow
  '#FF780A', // Orange
  '#F2495C', // Red
  '#8AB8FF', // Light Blue
  '#3274D9', // Blue
  '#B877D9', // Purple
  '#E0B400', // Gold
  '#96D98D', // Light Green
  '#FF9F30', // Light Orange
];

export const DeveloperLandscapePanel: React.FC<Props> = ({ data, width, height, options, replaceVariables }) => {
  const landscapeOptions = options.developerLandscape ?? {
    mapName: 'churn_throughput',
    showLabels: false,
    colorByTeam: false,
  };

  // Resolve focusIdentity variable (e.g. $identity_id)
  const rawFocusIdentity = landscapeOptions.focusIdentity;
  const focusIdentity = rawFocusIdentity ? replaceVariables(rawFocusIdentity) : undefined;


  const axisLabels = useMemo(() => {
    switch (landscapeOptions.mapName) {
      case 'cycle_throughput':
        return { x: 'Throughput', y: 'Cycle Time' };
      case 'wip_throughput':
        return { x: 'Throughput', y: 'WIP' };
      case 'churn_throughput':
      default:
        return { x: 'Throughput', y: 'Churn' };
    }
  }, [landscapeOptions.mapName]);

  const frame = getFrameWithFields(data.series, ['x_norm', 'y_norm']);

  if (!frame) {
    return (
      <PanelEmptyState
        title="Developer Landscape"
        message="Missing required fields to render the quadrant map."
        schema={[
          'Required fields:',
          '- x_norm (0-1)',
          '- y_norm (0-1)',
          'Optional fields:',
          '- identity_id',
          '- x_raw',
          '- y_raw',
          '- map_name',
          '- team_id',
          '- as_of_day',
        ]}
      />
    );
  }

  const xNormField = getField(frame, 'x_norm');
  const yNormField = getField(frame, 'y_norm');
  const xRawField = getField(frame, 'x_raw');
  const yRawField = getField(frame, 'y_raw');
  const mapField = getField(frame, 'map_name');
  const labelField = getField(frame, 'identity_id');
  const teamField = getField(frame, 'team_id');
  const asOfField = getField(frame, 'as_of_day');

  if (!xNormField || !yNormField) {
    return (
      <PanelEmptyState
        title="Developer Landscape"
        message="The x_norm and y_norm fields are required."
        schema={['Required fields:', '- x_norm (0-1)', '- y_norm (0-1)']}
      />
    );
  }

  const points = useMemo(() => {
    const result: Array<{
      xNorm: number;
      yNorm: number;
      xRaw?: number;
      yRaw?: number;
      label?: string;
      team?: string;
      asOf?: string;
    }> = [];
    const length = frame.length ?? xNormField.values.length;

    for (let i = 0; i < length; i++) {
      const xNorm = Number(getFieldValue<number>(xNormField, i));
      const yNorm = Number(getFieldValue<number>(yNormField, i));
      if (!Number.isFinite(xNorm) || !Number.isFinite(yNorm)) {
        continue;
      }

      if (mapField) {
        const mapValue = String(getFieldValue<string>(mapField, i) ?? '');
        if (mapValue && mapValue !== landscapeOptions.mapName) {
          continue;
        }
      }

      result.push({
        xNorm,
        yNorm,
        xRaw: xRawField ? Number(getFieldValue<number>(xRawField, i)) : undefined,
        yRaw: yRawField ? Number(getFieldValue<number>(yRawField, i)) : undefined,
        label: labelField ? String(getFieldValue<string>(labelField, i) ?? '') : undefined,
        team: teamField ? String(getFieldValue<string>(teamField, i) ?? '') : undefined,
        asOf: asOfField ? String(getFieldValue<string>(asOfField, i) ?? '') : undefined,
      });
    }
    return result;
  }, [
    frame.length,
    xNormField,
    yNormField,
    xRawField,
    yRawField,
    mapField,
    labelField,
    teamField,
    asOfField,
    landscapeOptions.mapName,
  ]);

  if (points.length === 0) {
    return (
      <PanelEmptyState
        title="Developer Landscape"
        message="No matching data for the selected map."
        schema={['Expected map_name values:', '- churn_throughput', '- cycle_throughput', '- wip_throughput']}
      />
    );
  }

  // Stable color mapping for identities and teams
  const { identityColorMap, teamColorMap } = useMemo(() => {
    const identities = Array.from(new Set(points.map((p) => p.label).filter(Boolean) as string[])).sort();
    const teams = Array.from(new Set(points.map((p) => p.team).filter(Boolean) as string[])).sort();

    const iMap = new Map<string, string>();
    identities.forEach((id, index) => iMap.set(id, palette[index % palette.length]));

    const tMap = new Map<string, string>();
    teams.forEach((team, index) => tMap.set(team, palette[index % palette.length]));

    return { identityColorMap: iMap, teamColorMap: tMap };
  }, [points]);

  const padding = 32;
  const plotWidth = Math.max(0, width - padding * 2);
  const plotHeight = Math.max(0, height - padding * 2);
  const midX = padding + plotWidth * 0.5;
  const midY = padding + plotHeight * 0.5;

  return (
    <div className={styles.wrapper}>
      <svg width={width} height={height}>
        <rect x={0} y={0} width={width} height={height} fill="transparent" />
        <line x1={midX} y1={padding} x2={midX} y2={padding + plotHeight} stroke="#3a4654" strokeWidth={1} />
        <line x1={padding} y1={midY} x2={padding + plotWidth} y2={midY} stroke="#3a4654" strokeWidth={1} />
        <rect
          x={padding}
          y={padding}
          width={plotWidth}
          height={plotHeight}
          fill="none"
          stroke="#2b3440"
          strokeWidth={1}
        />

        {/* Axis Labels */}
        <text x={midX} y={height - 8} className={styles.axisLabel}>
          {axisLabels.x}
        </text>
        <text transform={`translate(12, ${midY}) rotate(-90)`} className={styles.axisLabel}>
          {axisLabels.y}
        </text>

        {/* Render non-focused points first */}
        {points
          .filter((p) => (focusIdentity ? p.label !== focusIdentity : false))
          .map((point, index) => renderPoint(point, index, false))}

        {/* Render focused points last to ensure they are on top */}
        {points
          .filter((p) => (focusIdentity ? p.label === focusIdentity : true))
          .map((point, index) => renderPoint(point, index, true))}
      </svg>
    </div>
  );

  function renderPoint(point: any, index: number, isFocus: boolean) {
    const x = padding + Math.min(1, Math.max(0, point.xNorm)) * plotWidth;
    const y = padding + (1 - Math.min(1, Math.max(0, point.yNorm))) * plotHeight;

    const color = landscapeOptions.colorByTeam
      ? (point.team ? teamColorMap.get(point.team) : '#7f8fa3')
      : (point.label ? identityColorMap.get(point.label) : '#7f8fa3');

    const opacity = focusIdentity ? (isFocus ? 1 : 0.15) : 1;
    const showLabel = focusIdentity ? isFocus : landscapeOptions.showLabels;

    const tooltip = [
      point.label ? `ID: ${point.label}` : null,
      point.team ? `Team: ${point.team}` : null,
      point.asOf ? `As of: ${point.asOf}` : null,
      `x_raw: ${Number.isFinite(point.xRaw) ? point.xRaw : 'n/a'}`,
      `y_raw: ${Number.isFinite(point.yRaw) ? point.yRaw : 'n/a'}`,
      `x_norm: ${point.xNorm.toFixed(2)}`,
      `y_norm: ${point.yNorm.toFixed(2)}`,
    ]
      .filter(Boolean)
      .join('\n');

    // Simple heuristic to get a "name" from an email or ID
    const displayName = point.label
      ? point.label.includes('@')
        ? point.label
            .split('@')[0]
            .split(/[\._]/)
            .map((s: string) => s.charAt(0).toUpperCase() + s.slice(1))
            .join(' ')
        : point.label
      : '';

    return (
      <g key={`${point.label ?? 'point'}-${index}`} opacity={opacity}>
        <circle cx={x} cy={y} r={isFocus ? 6 : 4} fill={color ?? '#7f8fa3'} stroke={isFocus ? '#fff' : 'none'} strokeWidth={1}>
          <title>{tooltip}</title>
        </circle>
        {showLabel && point.label ? (
          <text
            x={x + 8}
            y={y + 4}
            className={styles.label}
            style={{
              fontWeight: isFocus ? 'bold' : 'normal',
              fontSize: isFocus ? '13px' : '11px',
              fill: isFocus ? '#fff' : '#dbe2ea',
            }}
          >
            {displayName}
          </text>
        ) : null}
      </g>
    );
  }
};
