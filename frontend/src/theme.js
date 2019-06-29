import { createMuiTheme } from '@material-ui/core/styles';
import blueGrey from '@material-ui/core/colors/blueGrey';
import amber from '@material-ui/core/colors/amber';

export default createMuiTheme({
    palette: {
        primary: blueGrey,
        secondary: amber,
    },
    typography: {
        htmlFontSize: 16,
        useNextVariants: true,
    },
});
