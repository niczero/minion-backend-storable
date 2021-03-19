use Mojo::Base -strict;
use Test::More;

use_ok 'Minion';

use_ok 'Minion::Backend::Storable';
diag "Testing Minion::Backend::Storable $Minion::Backend::Storable::VERSION";

use_ok 'Minion::Backend::Sereal';
diag "Testing Minion::Backend::Sereal $Minion::Backend::Sereal::VERSION";

diag "  with Minion $Minion::VERSION, Perl $], $^X";

like(Minion->VERSION, qr/^10\./, 'compatible version of Minion')
  or diag ' ** Compatible with Minion v10 only ** ';

done_testing();
