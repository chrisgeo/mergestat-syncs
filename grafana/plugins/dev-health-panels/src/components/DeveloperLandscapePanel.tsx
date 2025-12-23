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
};

const palette = ['#6b7c93', '#7a90a8', '#8aa0b5', '#9bb1c1', '#aebfcd', '#c2ced7'];

export const DeveloperLandscapePanel: React.FC<Props> = ({ data, width, height, options }) => {
  const landscapeOptions = options.developerLandscape ?? {
    mapName: 'churn_throughput',
    showLabels: false,
    colorByTeam: false,
  };
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

  const padding = 32;
  const plotWidth = Math.max(0, width - padding * 2);
  const plotHeight = Math.max(0, height - padding * 2);
  const midX = padding + plotWidth * 0.5;
  const midY = padding + plotHeight * 0.5;

  const teamColors = new Map<string, string>();
  let paletteIndex = 0;

  const getTeamColor = (team?: string) => {
    if (!team) {
      return '#7f8fa3';
    }
    if (!teamColors.has(team)) {
      teamColors.set(team, palette[paletteIndex % palette.length]);
      paletteIndex += 1;
    }
    return teamColors.get(team) ?? '#7f8fa3';
  };

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
        {points.map((point, index) => {
          const x = padding + Math.min(1, Math.max(0, point.xNorm)) * plotWidth;
          const y = padding + (1 - Math.min(1, Math.max(0, point.yNorm))) * plotHeight;
          const color = landscapeOptions.colorByTeam ? getTeamColor(point.team) : '#7f8fa3';
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

          return (
            <g key={`${point.label ?? 'point'}-${index}`}>
              <circle cx={x} cy={y} r={5} fill={color}>
                <title>{tooltip}</title>
              </circle>
              {landscapeOptions.showLabels && point.label ? (
                <text x={x + 6} y={y - 6} className={styles.label}>
                  {point.label}
                </text>
              ) : null}
            </g>
          );
        })}
      </svg>
    </div>
  );
};
