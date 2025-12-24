export type PanelMode = 'developerLandscape' | 'hotspotExplorer' | 'investmentFlow';

export type DeveloperMapName = 'churn_throughput' | 'cycle_throughput' | 'wip_throughput';

export interface DeveloperLandscapeOptions {
  mapName: DeveloperMapName;
  showLabels: boolean;
  colorByTeam: boolean;
  focusIdentity?: string;
}

export interface HotspotExplorerOptions {
  defaultSortByRisk: boolean;
}

export type InvestmentTarget = 'project_stream' | 'outcome' | 'target';

export interface InvestmentFlowOptions {
  timeWindowDays: 30 | 60 | 90;
  valueField: string;
  sourceField: string;
  targetField: InvestmentTarget;
  dayField: string;
}

export interface DevHealthOptions {
  mode: PanelMode;
  developerLandscape: DeveloperLandscapeOptions;
  hotspotExplorer: HotspotExplorerOptions;
  investmentFlow: InvestmentFlowOptions;
}
