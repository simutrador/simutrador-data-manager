/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './src/**/*.{html,ts}',
  ],
  plugins: [
    require('daisyui'),
  ],
  daisyui: {
    themes: 'all',
  },
};

