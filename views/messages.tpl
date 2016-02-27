% if defined('errors'):
    % for error in errors:
    <div class="alert alert-danger" role="alert">{{error}}</div>
    % end
% end
