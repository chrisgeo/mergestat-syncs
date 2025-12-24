import { DataFrame, Field } from '@grafana/data';

const normalizeName = (name: string) => name.toLowerCase();

export const getField = (frame: DataFrame, name: string): Field | undefined => {
  const direct = frame.fields.find((field) => field.name === name);
  if (direct) {
    return direct;
  }
  const normalized = normalizeName(name);
  return frame.fields.find((field) => normalizeName(field.name) === normalized);
};

export const frameHasFields = (frame: DataFrame, names: string[]): boolean => {
  return names.every((name) => Boolean(getField(frame, name)));
};

export const getFrameWithFields = (frames: DataFrame[], names: string[]): DataFrame | undefined => {
  return frames.find((frame) => frameHasFields(frame, names));
};

export const getFieldValue = <T>(field: Field, index: number): T | undefined => {
  const values = field.values;
  if (!values) {
    return undefined;
  }
  return values[index] as T;
};
