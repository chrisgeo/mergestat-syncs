import React, { useMemo } from 'react';
import { PanelProps, ThemeContext } from '@grafana/data';
import { useTheme2 } from '@grafana/ui';
import { DevHealthOptions } from '../types';
import { DeveloperLandscapePanel } from './DeveloperLandscapePanel';
import { HotspotExplorerPanel } from './HotspotExplorerPanel';
import { InvestmentFlowPanel } from './InvestmentFlowPanel';
import { getDevHealthTheme } from '../theme/devHealthTheme';

interface Props extends PanelProps<DevHealthOptions> {}

export const DevHealthPanel: React.FC<Props> = (props) => {
  const baseTheme = useTheme2();
  const theme = useMemo(() => getDevHealthTheme(baseTheme), [baseTheme]);

  const panel = (() => {
    switch (props.options.mode) {
      case 'hotspotExplorer':
        return <HotspotExplorerPanel {...props} />;
      case 'investmentFlow':
        return <InvestmentFlowPanel {...props} />;
      case 'developerLandscape':
      default:
        return <DeveloperLandscapePanel {...props} />;
    }
  })();

  return <ThemeContext.Provider value={theme}>{panel}</ThemeContext.Provider>;
};
