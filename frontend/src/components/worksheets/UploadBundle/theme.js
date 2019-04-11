import { createMuiTheme } from '@material-ui/core/styles';

const theme = createMuiTheme({
  palette: {
    primary: { main: '#225EA8' },
    secondary: { main: '#ffaf7d' },
  },
  typography: { useNextVariants: true },
});

export default theme;
