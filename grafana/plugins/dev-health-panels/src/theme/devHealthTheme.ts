import { createTheme, GrafanaTheme2 } from '@grafana/data';

export const devHealthPalette = [
  '#73BF69',
  '#F2CC0C',
  '#FF780A',
  '#F2495C',
  '#8AB8FF',
  '#3274D9',
  '#B877D9',
  '#E0B400',
  '#96D98D',
  '#FF9F30',
];

export const getDevHealthTheme = (baseTheme: GrafanaTheme2): GrafanaTheme2 => {
  const theme = createTheme({
    colors: {
      mode: baseTheme.isDark ? 'dark' : 'light',
      primary: baseTheme.colors.primary,
      secondary: baseTheme.colors.secondary,
      info: baseTheme.colors.info,
      warning: baseTheme.colors.warning,
      success: baseTheme.colors.success,
      error: baseTheme.colors.error,
      background: baseTheme.colors.background,
      text: baseTheme.colors.text,
      border: baseTheme.colors.border,
      action: baseTheme.colors.action,
      gradients: baseTheme.colors.gradients,
    },
    typography: {
      fontFamily: baseTheme.typography.fontFamily,
      fontFamilyMonospace: baseTheme.typography.fontFamilyMonospace,
      fontSize: baseTheme.typography.fontSize,
    },
    spacing: {
      gridSize: baseTheme.spacing.gridSize,
    },
  });

  return {
    ...theme,
    visualization: {
      ...theme.visualization,
      palette: devHealthPalette,
    },
  };
};
