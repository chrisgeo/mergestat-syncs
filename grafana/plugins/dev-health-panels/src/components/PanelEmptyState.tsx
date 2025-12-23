import React from 'react';
import { css } from '@emotion/css';

interface Props {
  title: string;
  message: string;
  schema: string[];
}

const styles = {
  wrapper: css`
    padding: 16px;
    font-family: Open Sans, Helvetica, Arial, sans-serif;
    color: #cfd6df;
  `,
  title: css`
    font-size: 16px;
    margin-bottom: 8px;
  `,
  message: css`
    font-size: 13px;
    margin-bottom: 12px;
    color: #9aa7b2;
  `,
  schema: css`
    background: rgba(255, 255, 255, 0.05);
    padding: 10px 12px;
    border-radius: 6px;
    font-family: Menlo, Monaco, Consolas, 'Courier New', monospace;
    font-size: 12px;
    white-space: pre-wrap;
  `,
};

export const PanelEmptyState: React.FC<Props> = ({ title, message, schema }) => {
  return (
    <div className={styles.wrapper}>
      <div className={styles.title}>{title}</div>
      <div className={styles.message}>{message}</div>
      <div className={styles.schema}>{schema.join('\n')}</div>
    </div>
  );
};
