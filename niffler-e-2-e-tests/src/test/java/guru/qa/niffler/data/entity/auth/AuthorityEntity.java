package guru.qa.niffler.data.entity.auth;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
public class AuthorityEntity {
    private UUID id;
    private Authority authority;
    private UUID userId;
}
