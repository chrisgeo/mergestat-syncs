import { PanelPlugin } from '@grafana/data';
import { DevHealthOptions } from './types';
import { DevHealthPanel } from './components/DevHealthPanel';

export const plugin = new PanelPlugin<DevHealthOptions>(DevHealthPanel).setPanelOptions((builder) => {
  return builder
    .addSelect({
      path: 'mode',
      name: 'Panel',
      description: 'Select the panel view to render.',
      defaultValue: 'developerLandscape',
      settings: {
        options: [
          { value: 'developerLandscape', label: 'Developer Landscape' },
          { value: 'hotspotExplorer', label: 'Hotspot Explorer' },
          { value: 'investmentFlow', label: 'Investment Flow' },
        ],
      },
    })
    .addSelect({
      path: 'developerLandscape.mapName',
      name: 'Map',
      defaultValue: 'churn_throughput',
      settings: {
        options: [
          { value: 'churn_throughput', label: 'Churn vs Throughput' },
          { value: 'cycle_throughput', label: 'Cycle Time vs Throughput' },
          { value: 'wip_throughput', label: 'WIP vs Throughput' },
        ],
      },
      showIf: (config) => config.mode === 'developerLandscape',
    })
    .addBooleanSwitch({
      path: 'developerLandscape.showLabels',
      name: 'Show labels',
      defaultValue: false,
      showIf: (config) => config.mode === 'developerLandscape',
    })
    .addBooleanSwitch({
      path: 'developerLandscape.colorByTeam',
      name: 'Color by team',
      defaultValue: false,
      showIf: (config) => config.mode === 'developerLandscape',
    })
    .addBooleanSwitch({
      path: 'hotspotExplorer.defaultSortByRisk',
      name: 'Sort by risk score by default',
      defaultValue: true,
      showIf: (config) => config.mode === 'hotspotExplorer',
    })
    .addSelect({
      path: 'investmentFlow.timeWindowDays',
      name: 'Time window',
      defaultValue: 30,
      settings: {
        options: [
          { value: 30, label: 'Last 30 days' },
          { value: 60, label: 'Last 60 days' },
          { value: 90, label: 'Last 90 days' },
        ],
      },
      showIf: (config) => config.mode === 'investmentFlow',
    })
    .addTextInput({
      path: 'investmentFlow.valueField',
      name: 'Value field',
      defaultValue: 'value',
      description: 'Field name that represents effort (value, delivery_units, churn_loc, count).',
      showIf: (config) => config.mode === 'investmentFlow',
    })
    .addTextInput({
      path: 'investmentFlow.sourceField',
      name: 'Source field',
      defaultValue: 'source',
      showIf: (config) => config.mode === 'investmentFlow',
    })
    .addSelect({
      path: 'investmentFlow.targetField',
      name: 'Group by',
      defaultValue: 'project_stream',
      settings: {
        options: [
          { value: 'project_stream', label: 'Project stream' },
          { value: 'outcome', label: 'Outcome' },
          { value: 'target', label: 'Target' },
        ],
      },
      showIf: (config) => config.mode === 'investmentFlow',
    })
    .addTextInput({
      path: 'investmentFlow.dayField',
      name: 'Day field',
      defaultValue: 'day',
      showIf: (config) => config.mode === 'investmentFlow',
    });
});
