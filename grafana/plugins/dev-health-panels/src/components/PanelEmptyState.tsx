import React from 'react';
import { css } from '@emotion/css';
import { GrafanaTheme2 } from '@grafana/data';
import { useStyles2 } from '@grafana/ui';

interface Props {
  title: string;
  message: string;
  schema: string[];
}

const getStyles = (theme: GrafanaTheme2) => ({
  wrapper: css`
    padding: 16px;
    font-family: ${theme.typography.fontFamily};
    color: ${theme.colors.text.primary};
  `,
  title: css`
    font-size: 16px;
    margin-bottom: 8px;
  `,
  message: css`
    font-size: 13px;
    margin-bottom: 12px;
    color: ${theme.colors.text.secondary};
  `,
  schema: css`
    background: ${theme.colors.background.secondary};
    padding: 10px 12px;
    border-radius: ${theme.shape.radius.default};
    font-family: ${theme.typography.fontFamilyMonospace};
    font-size: 12px;
    white-space: pre-wrap;
  `,
});

export const PanelEmptyState: React.FC<Props> = ({ title, message, schema }) => {
  const styles = useStyles2(getStyles);
  return (
    <div className={styles.wrapper}>
      <div className={styles.title}>{title}</div>
      <div className={styles.message}>{message}</div>
      <div className={styles.schema}>{schema.join('\n')}</div>
    </div>
  );
};
