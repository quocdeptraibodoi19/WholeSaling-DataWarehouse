/** @type {import('tailwindcss').Config} */
export default {
  purge: ['./index.html', './src/**/*.{vue,js,ts,jsx,tsx}'],
  content: [],  // Note: Starting from Tailwind CSS v3.0, use `content` instead of `purge`
  theme: {
    extend: {
      colors: {
        'primary': '#3D5AF3',
        'disabled': '#A5C3F0',
        'text': '#052759',
        'text-secondary': '#60728D',
        'background-accent': '#F2F8FF',
        'background-accent-2': '#F7FCFF',
        'background-gray': '#FCFDFF',
        'white': '#fff',
        'border': '#E1E3EF',
        'border-2': '#E6E8F5'
      },
      maxWidth: {
        'custom': '1376px',
      }
    },
  },
  plugins: [],
}
