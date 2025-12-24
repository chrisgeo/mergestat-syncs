import React, { useMemo } from 'react';
import { css } from '@emotion/css';
import { GrafanaTheme2, PanelProps } from '@grafana/data';
import { useStyles2, useTheme2 } from '@grafana/ui';
import { DevHealthOptions } from '../types';
import { getField, getFieldValue, getFrameWithFields } from './dataFrame';
import { PanelEmptyState } from './PanelEmptyState';

interface Props extends PanelProps<DevHealthOptions> {}

const getStyles = (theme: GrafanaTheme2) => ({
  wrapper: css`
    width: 100%;
    height: 100%;
    font-family: ${theme.typography.fontFamily};
  `,
  label: css`
    font-size: 11px;
    fill: ${theme.colors.text.secondary};
  `,
});

type FlowRecord = {
  source: string;
  target: string;
  value: number;
  day?: string;
};

export const InvestmentFlowPanel: React.FC<Props> = ({ data, width, height, options }) => {
  const theme = useTheme2();
  const styles = useStyles2(getStyles);

  const investmentOptions = useMemo(() => options.investmentFlow ?? {
    timeWindowDays: 30,
    valueField: 'value',
    sourceField: 'source',
    targetField: 'target',
    dayField: 'day',
  }, [options.investmentFlow]);

  const sourceFieldName = investmentOptions.sourceField || 'source';
  const targetFieldName = investmentOptions.targetField || 'target';
  const valueFieldName = investmentOptions.valueField || 'value';
  const dayFieldName = investmentOptions.dayField || 'day';

  const frame = getFrameWithFields(data.series, [sourceFieldName, valueFieldName]);
  const targetExists = frame ? Boolean(getField(frame, targetFieldName)) : false;

  const flows = useMemo(() => {
    if (!frame) {
      return [];
    }
    const sourceField = getField(frame, sourceFieldName);
    const targetField = getField(frame, targetFieldName);
    const valueField = getField(frame, valueFieldName);
    const dayField = getField(frame, dayFieldName);

    if (!sourceField || !valueField) {
      return [];
    }

    const windowMs = investmentOptions.timeWindowDays * 24 * 60 * 60 * 1000;
    const cutoff = Date.now() - windowMs;
    const length = frame.length ?? sourceField.values.length;
    const rows: FlowRecord[] = [];

    for (let i = 0; i < length; i++) {
      const source = String(getFieldValue<string>(sourceField, i) ?? '');
      if (!source) {
        continue;
      }
      const target = targetField ? String(getFieldValue<string>(targetField, i) ?? '') : 'Total';
      const value = Number(getFieldValue<number>(valueField, i));
      if (!Number.isFinite(value)) {
        continue;
      }
      let day: string | undefined;
      if (dayField) {
        day = String(getFieldValue<string>(dayField, i) ?? '');
        if (day) {
          const time = Date.parse(day);
          if (Number.isFinite(time) && time < cutoff) {
            continue;
          }
        }
      }
      rows.push({ source, target, value, day });
    }
    return rows;
  }, [frame, investmentOptions, sourceFieldName, targetFieldName, valueFieldName, dayFieldName]);

  // Generate color map based on unique sources and targets found in flows
  const colorMap = useMemo(() => {
    const allNames = new Set<string>();
    flows.forEach((f) => {
      allNames.add(f.source);
      allNames.add(f.target);
    });
    const sortedNames = Array.from(allNames).sort();
    const map = new Map<string, string>();
    const palette = theme.visualization.palette;
    sortedNames.forEach((name, index) => {
      const paletteColor = palette[index % palette.length];
      map.set(name, theme.visualization.getColorByName(paletteColor));
    });
    return map;
  }, [flows, theme]);

  const { nodesLeft, nodesRight, links, total } = useMemo(() => {
    const leftTotals = new Map<string, number>();
    const rightTotals = new Map<string, number>();
    const linkMap = new Map<string, number>();

    for (const flow of flows) {
      leftTotals.set(flow.source, (leftTotals.get(flow.source) ?? 0) + flow.value);
      rightTotals.set(flow.target, (rightTotals.get(flow.target) ?? 0) + flow.value);
      const key = `${flow.source}|||${flow.target}`;
      linkMap.set(key, (linkMap.get(key) ?? 0) + flow.value);
    }

    const nodesLeft = Array.from(leftTotals.entries()).map(([name, value]) => ({ name, value }));
    const nodesRight = Array.from(rightTotals.entries()).map(([name, value]) => ({ name, value }));
    const links = Array.from(linkMap.entries()).map(([key, value]) => {
      const [source, target] = key.split('|||');
      return { source, target, value };
    });
    const total = nodesLeft.reduce((sum, node) => sum + node.value, 0);
    return { nodesLeft, nodesRight, links, total };
  }, [flows]);

  if (!frame) {
    return (
      <PanelEmptyState
        title="Investment Flow"
        message="Missing required fields to render investment flow."
        schema={[
          'Required fields:',
          '- source (investment_area)',
          `- ${valueFieldName}`,
          'Optional fields:',
          `- ${targetFieldName} (project_stream or outcome)`,
          `- ${dayFieldName}`,
        ]}
      />
    );
  }

  if (flows.length === 0) {
    return (
      <PanelEmptyState
        title="Investment Flow"
        message="No rows matched the selected time window."
        schema={[`Time window: last ${investmentOptions.timeWindowDays} days`]}
      />
    );
  }

  if (!targetExists) {
    const totals = new Map<string, number>();
    for (const flow of flows) {
      totals.set(flow.source, (totals.get(flow.source) ?? 0) + flow.value);
    }
    const totalValue = Array.from(totals.values()).reduce((sum, value) => sum + value, 0);
    const barWidth = Math.max(0, width - 120);
    const barHeight = 18;

    return (
      <div className={styles.wrapper}>
        <svg width={width} height={height}>
          {Array.from(totals.entries()).map(([source, value], index) => {
            const y = 20 + index * (barHeight + 12);
            const segmentWidth = totalValue ? (value / totalValue) * barWidth : 0;
            const color = colorMap.get(source) ?? theme.visualization.getColorByName(theme.visualization.palette[0]);
            return (
              <g key={source}>
                <text x={8} y={y + barHeight - 4} className={styles.label}>
                  {source}
                </text>
                <rect x={110} y={y} width={segmentWidth} height={barHeight} fill={color}>
                  <title>
                    {source}: {value.toFixed(2)}
                  </title>
                </rect>
              </g>
            );
          })}
        </svg>
      </div>
    );
  }

  const padding = 24;
  const plotHeight = Math.max(0, height - padding * 2);
  const leftX = padding;
  const rightX = Math.max(padding + 220, width - padding - 220);
  const nodeWidth = 12;
  const gap = 12;
  const scale = total > 0 ? (plotHeight - gap * (nodesLeft.length + 1)) / total : 0;

  const leftPositions = new Map<string, { y: number; height: number }>();
  const rightPositions = new Map<string, { y: number; height: number }>();

  let currentY = padding + gap;
  nodesLeft.forEach((node) => {
    const height = Math.max(6, node.value * scale);
    leftPositions.set(node.name, { y: currentY, height });
    currentY += height + gap;
  });

  currentY = padding + gap;
  nodesRight.forEach((node) => {
    const height = Math.max(6, node.value * scale);
    rightPositions.set(node.name, { y: currentY, height });
    currentY += height + gap;
  });

  const sourceOffsets = new Map<string, number>();
  const targetOffsets = new Map<string, number>();

  return (
    <div className={styles.wrapper}>
      <svg width={width} height={height}>
        {links.map((link) => {
          const sourcePos = leftPositions.get(link.source);
          const targetPos = rightPositions.get(link.target);
          if (!sourcePos || !targetPos) {
            return null;
          }

          const sourceOffset = sourceOffsets.get(link.source) ?? 0;
          const targetOffset = targetOffsets.get(link.target) ?? 0;
          const thickness = Math.max(2, link.value * scale);

          const sourceY = sourcePos.y + sourceOffset + thickness / 2;
          const targetY = targetPos.y + targetOffset + thickness / 2;
          sourceOffsets.set(link.source, sourceOffset + thickness);
          targetOffsets.set(link.target, targetOffset + thickness);

          const startX = leftX + nodeWidth;
          const endX = rightX;
          const controlX = (startX + endX) / 2;
          const path = `M ${startX} ${sourceY} C ${controlX} ${sourceY}, ${controlX} ${targetY}, ${endX} ${targetY}`;
          
          // If we have a single source, color links by target to show the breakdown
          const colorKey = nodesLeft.length === 1 ? link.target : link.source;
          const color = colorMap.get(colorKey) ?? theme.visualization.getColorByName(theme.visualization.palette[0]);
          const percent = total > 0 ? (link.value / total) * 100 : 0;

          return (
            <path
              key={`${link.source}-${link.target}`}
              d={path}
              stroke={color}
              strokeWidth={thickness}
              fill="none"
              opacity={0.5}
            >
              <title>
                {link.source} → {link.target}: {link.value.toFixed(2)} ({percent.toFixed(1)}%)
              </title>
            </path>
          );
        })}

        {nodesLeft.map((node) => {
          const pos = leftPositions.get(node.name);
          if (!pos) {
            return null;
          }
          const color = colorMap.get(node.name) ?? theme.visualization.getColorByName(theme.visualization.palette[0]);
          return (
            <g key={node.name}>
              <rect x={leftX} y={pos.y} width={nodeWidth} height={pos.height} fill={color} />
              <text x={leftX + nodeWidth + 6} y={pos.y + pos.height / 2 + 4} className={styles.label}>
                {node.name}
              </text>
            </g>
          );
        })}

        {nodesRight.map((node) => {
          const pos = rightPositions.get(node.name);
          if (!pos) {
            return null;
          }
          const color = colorMap.get(node.name) ?? theme.visualization.getColorByName(theme.visualization.palette[0]);
          return (
            <g key={node.name}>
              <rect x={rightX} y={pos.y} width={nodeWidth} height={pos.height} fill={color} />
              <text x={rightX + nodeWidth + 6} y={pos.y + pos.height / 2 + 4} className={styles.label}>
                {node.name}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
};
