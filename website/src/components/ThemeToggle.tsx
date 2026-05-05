import React from 'react';
import { useColorMode } from '@docusaurus/theme-common';
import styles from './ThemeToggle.module.css';

export default function ThemeToggle(): JSX.Element {
  const { colorMode, setColorMode } = useColorMode();

  return (
    <button
      className={styles.themeToggle}
      onClick={() => setColorMode(colorMode === 'light' ? 'dark' : 'light')}
      aria-label="Toggle theme"
      title={colorMode === 'light' ? 'Switch to dark mode' : 'Switch to light mode'}
    >
      {colorMode === 'light' ? (
        <span className={styles.icon}>🌙</span>
      ) : (
        <span className={styles.icon}>☀️</span>
      )}
    </button>
  );
}
