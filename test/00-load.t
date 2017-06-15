use Mojo::Base -strict;
use Test::More;

use_ok 'Minion';
diag "Testing Minion $Minion::VERSION, Perl $], $^X";
use_ok 'Minion::Backend::Storable';

like(Minion->VERSION, qr/^5\./, 'compatible version of Minion')
  or diag ' ** Compatible with Minion v5 only ** ';

done_testing();
