% if defined('message'):
    <div class="alert alert-success" role="alert">{{message}}</div>
% end
% if defined('errors'):
    % for error in errors:
    <div class="alert alert-danger" role="alert">{{error}}</div>
    % end
% end
