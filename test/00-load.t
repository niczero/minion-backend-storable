use Mojo::Base -strict;
use Test::More;

use_ok 'Minion';
diag "Testing Minion $Minion::VERSION, Perl $], $^X";
use_ok 'Minion::Backend::Storable';

like(Minion->VERSION, qr/^6\./, 'compatible version of Minion')
  or diag ' ** Compatible with Minion v6 only ** ';

done_testing();
