import React from 'react';
import { PanelProps } from '@grafana/data';
import { DevHealthOptions } from '../types';
import { DeveloperLandscapePanel } from './DeveloperLandscapePanel';
import { HotspotExplorerPanel } from './HotspotExplorerPanel';
import { InvestmentFlowPanel } from './InvestmentFlowPanel';

interface Props extends PanelProps<DevHealthOptions> {}

export const DevHealthPanel: React.FC<Props> = (props) => {
  switch (props.options.mode) {
    case 'hotspotExplorer':
      return <HotspotExplorerPanel {...props} />;
    case 'investmentFlow':
      return <InvestmentFlowPanel {...props} />;
    case 'developerLandscape':
    default:
      return <DeveloperLandscapePanel {...props} />;
  }
};
